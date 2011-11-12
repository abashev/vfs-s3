package com.intridea.io.vfs.provider.s3;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

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
    @Override
    public FileName parseUri(final VfsComponentContext context,
            final FileName base, final String filename)
            throws FileSystemException {
        StringBuilder name = new StringBuilder();

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
