package com.github.vfss3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.util.MonitorInputStream;
import org.apache.commons.vfs2.util.MonitorOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 * Class that manages the lifecycle of temporary files used to buffer S3 file content locally
 *
 * @author Shon Vella
 */
class S3TempFile {
    private static final Log logger = LogFactory.getLog(S3TempFile.class);

    private final Path tempFile;
    private int useCount = 1;

    private String eTag;

    /**
     * Creates a temporary file
     *
     * @throws IOException if unable to create the file
     */
    S3TempFile() throws IOException {
        tempFile = Files.createTempFile("vfs.", ".s3");
    }

    Path getPath() {
        return tempFile;
    }

    /**
     * Increments the use count
     *
     * @return the updated use count
     * @throws FileNotFoundException if use count had already been decremented to 0
     */
    synchronized int use()
            throws FileNotFoundException {
        if (useCount > 0) {
            return ++useCount;
        } else {
            throw new FileNotFoundException("File no longer available");
        }
    }

    /**
     * Decrements the use count
     *
     * @return the updated use count
     */
    synchronized int release() {
        if (useCount > 0) {
            if (--useCount == 0) {
                // useCount has gone to 0, delete the file
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {
                    logger.warn("Error deleting temp file: ", e);
                }
            }
        }
        return useCount;
    }

    /**
     * Gets the use count
     *
     * @return the use count
     */
    synchronized int getUseCount() {
        return useCount;
    }

    /**
     * Gets a file channel
     *
     * @return the file channel
     * @throws IOException if use count had already been decremented to 0, or unable to open file
     */
    synchronized FileChannel getFileChannel(OpenOption... openOptions)
            throws IOException {
        if (useCount > 0) {
            FileChannel fileChannel = FileChannel.open(tempFile, openOptions);
            use();
            return fileChannel;
        } else {
            throw new FileNotFoundException("File no longer available");
        }
    }

    /**
     * Gets an input stream
     *
     * @return the input stream
     * @throws IOException if use count had already been decremented to 0, or unable to open file
     */
    synchronized InputStream getInputStream()
            throws IOException {
        if (useCount > 0) {
            InputStream inputStream = new MonitorInputStream(Files.newInputStream(tempFile)) {
                @Override
                protected void onClose() throws IOException {
                    super.onClose();
                    release();
                }
            };
            use();
            return inputStream;
        } else {
            throw new FileNotFoundException("File no longer available");
        }
    }

    /**
     * Gets an output stream
     *
     * @return the output stream
     * @throws IOException if use count had already been decremented to 0, or unable to open file
     */
    synchronized OutputStream getOutputStream()
            throws IOException {
        if (useCount > 0) {
            OutputStream outputStream = new MonitorOutputStream(Files.newOutputStream(tempFile)) {
                @Override
                protected void onClose() throws IOException {
                    super.onClose();
                    release();
                }
            };
            use();
            return outputStream;
        } else {
            throw new FileNotFoundException("File no longer available");
        }
    }


    synchronized String getAbsolutePath() {
        if (useCount > 0) {
            return tempFile.toAbsolutePath().toString();
        } else {
            return null;
        }
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    @Override
    protected void finalize() throws Throwable {
        if (useCount > 0) {
            useCount = 1;
            release();
        }
        super.finalize();
    }
}
