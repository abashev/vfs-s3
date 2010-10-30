package com.intridea.io.vfs.provider.s3;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.AbstractFileNameParser;
import org.apache.commons.vfs.provider.UriParser;
import org.apache.commons.vfs.provider.VfsComponentContext;

/**
 * @author Matthias L. Jugel
 */
public class S3FileNameParser extends AbstractFileNameParser {
    /**
     * S3 file name parser instance
     */
    private static final S3FileNameParser instance = new S3FileNameParser();

    /**
     * Gets singleton
     * @return
     */
    public static S3FileNameParser getInstance() {
        return instance;
    }

    private S3FileNameParser() {
    }

    /**
     * Parses URI and constructs S3 file name.
     */
    public FileName parseUri(final VfsComponentContext context,
            final FileName base, final String filename)
            throws FileSystemException {
        StringBuffer name = new StringBuffer();

        String scheme = UriParser.extractScheme(filename, name);
        UriParser.canonicalizePath(name, 0, name.length(), this);

        // Normalize separators in the path
        UriParser.fixSeparators(name);

        // Normalise the path
        FileType fileType = UriParser.normalisePath(name);

        // Extract bucket name
        final String bucketName = UriParser.extractFirstElement(name);

        return new S3FileName(scheme, bucketName, name.toString(), fileType);
    }

}
