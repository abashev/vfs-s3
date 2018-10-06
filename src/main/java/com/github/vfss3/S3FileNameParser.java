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

/**
 * @author Matthias L. Jugel
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileNameParser extends AbstractFileNameParser {
    private final Logger log = LoggerFactory.getLogger(S3FileNameParser.class);

    private static final Pattern PATH_STYLE = compile("s3-?([a-zA-Z0-9\\-]*)\\.amazonaws\\.com");
    private static final Pattern HOST_STYLE = compile("([a-zA-Z0-9\\-]+)\\.s3-?([a-zA-Z0-9\\-]*)\\.amazonaws\\.com");

    private static final Pattern EXTRACT_REGION = compile("s3-?([a-zA-Z0-9\\-]*)\\.amazonaws\\.com$");

    private static final Pattern PATH = compile("^/([^/]+)/?(.*)");

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

        if (uri.getHost().endsWith(".amazonaws.com")) {
            // Standard AWS endpoint
            final Matcher pathStyleMatcher = PATH_STYLE.matcher(uri.getHost());

            if (pathStyleMatcher.matches()) {
                checkRegion(pathStyleMatcher.group(1));

                final Matcher pathMatcher = PATH.matcher(uri.getPath());

                if (pathMatcher.matches()) {
                    StringBuilder sb = new StringBuilder(pathMatcher.group(2));

                    UriParser.fixSeparators(sb);

                    FileType fileType = UriParser.normalisePath(sb);

                    S3FileName file = new S3FileName(uri.getHost(), pathMatcher.group(1), sb.toString(), fileType);

                    log.debug("From uri {} got {}", filename, file);

                    return file;
                } else {
                    throw new FileSystemException("Not able to find bucket inside [" + filename + "]");
                }
            }

            final Matcher hostStyleMatcher = HOST_STYLE.matcher(uri.getHost());

            if (hostStyleMatcher.matches()) {
                checkRegion(hostStyleMatcher.group(2));

                StringBuilder sb = new StringBuilder(uri.getPath());

                UriParser.fixSeparators(sb);

                FileType fileType = UriParser.normalisePath(sb);

                S3FileName file = new S3FileName(uri.getHost(), sb.toString(), fileType);

                log.debug("From uri {} got {}", filename, file);

                return file;
            }

            throw new FileSystemException("Not able to parse url [" + filename + "]");
        } else {
            // Custom endpoint like localstack
            StringBuilder host = new StringBuilder(uri.getHost());

            if (uri.getPort() != (-1)) {
                host.append(':').append(uri.getPort());
            }

            final Matcher pathMatcher = PATH.matcher(uri.getPath());

            if (pathMatcher.matches()) {
                StringBuilder sb = new StringBuilder(pathMatcher.group(2));

                UriParser.fixSeparators(sb);

                FileType fileType = UriParser.normalisePath(sb);

                S3FileName file = new S3FileName(host, pathMatcher.group(1), sb.toString(), fileType);

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
        if (host.endsWith(".amazonaws.com")) {
            // Standard AWS endpoint
            final Matcher extractRegion = EXTRACT_REGION.matcher(host);

            if (extractRegion.find()) {
                String candidate = extractRegion.group(1);
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
        }

        return defaultRegion;
    }

    /**
     * Extract bucket id from S3 file name. Use path prefix or host
     *
     * @param fileName
     * @return
     */
    public String bucketFromFileName(S3FileName fileName) throws FileSystemException {
        if (fileName.isPathPrefixNotEmpty()) {
            return fileName.getPathPrefix();
        }

        final Matcher hostStyleMatcher = HOST_STYLE.matcher(fileName.getHostAndPort());

        if (hostStyleMatcher.matches()) {
            return hostStyleMatcher.group(1);
        }

        throw new FileSystemException("Not able to find bucket inside " + fileName);
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
}
