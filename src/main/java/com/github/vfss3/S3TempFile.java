package com.github.vfss3;

import org.apache.commons.vfs2.util.MonitorInputStream;
import org.apache.commons.vfs2.util.MonitorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that manages the lifecycle of temporary files used to buffer S3 file content locally
 *
 * @author Shon Vella
 */
class S3TempFile {
    private static final Logger logger = LoggerFactory.getLogger(S3TempFile.class);

    private final Path tempFile;
    private final AtomicInteger useCounter = new AtomicInteger(1);

    private String md5;

    /**
     * Creates a temporary file
     *
     * @throws IOException if unable to create the file
     */
    S3TempFile() throws IOException {
        this.tempFile = Files.createTempFile("vfs.", ".s3");
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
    void use() throws FileNotFoundException {
        if (useCounter.get() <= 0) {
            throw new FileNotFoundException("File no longer available");
        }

        logger.debug("Increment counter for {}", tempFile);

        useCounter.getAndIncrement();
    }

    /**
     * Decrements the use count
     *
     * @return the updated use count
     */
    void release() {
        if (useCounter.decrementAndGet() <= 0) {
            logger.debug("Counter is zero for {}", tempFile);

            // useCount has gone to 0, delete the file
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                logger.warn("Error deleting temp file: ", e);
            }
        }
    }

    /**
     * Gets a file channel
     *
     * @return the file channel
     * @throws IOException if use count had already been decremented to 0, or unable to open file
     */
    FileChannel getFileChannel(OpenOption... openOptions) throws IOException {
        if (useCounter.get() > 0) {
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
    InputStream getInputStream() throws IOException {
        if (useCounter.get() > 0) {
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
    OutputStream getOutputStream() throws IOException {
        if (useCounter.get() > 0) {
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

    String getAbsolutePath() {
        if (useCounter.get() > 0) {
            return tempFile.toAbsolutePath().toString();
        } else {
            return null;
        }
    }

    public String getMD5Hash() {
        return md5;
    }

    public void setMD5Hash(String md5) {
        this.md5 = md5;
    }

    @Override
    protected void finalize() throws Throwable {
        if (useCounter.get() > 0) {
            useCounter.set(1);

            release();
        }

        super.finalize();
    }
}
