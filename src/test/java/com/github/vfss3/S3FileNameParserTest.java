package com.github.vfss3;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.assertj.core.api.AssertDelegateTarget;
import org.testng.annotations.Test;

import static org.apache.commons.vfs2.FileType.FILE;
import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileNameParserTest {
    @Test
    public void checkHostedStyleUrl() throws FileSystemException {
        assertThat(parse("s3://bucket.s3.amazonaws.com")).
                hasHostAndPort("bucket.s3.amazonaws.com").
                hasType(FOLDER);
        assertThat(parse("s3://bucket.s3-eu-west-1.amazonaws.com")).
                hasHostAndPort("bucket.s3-eu-west-1.amazonaws.com").
                hasType(FOLDER);
        assertThat(parse("s3://bucket.s3-eu-west-1.amazonaws.com/some_file")).
                hasHostAndPort("bucket.s3-eu-west-1.amazonaws.com").
                hasType(FILE);
    }

    @Test
    public void checkPathStyleUrl() throws FileSystemException {
        assertThat(parse("s3://s3.amazonaws.com/bucket")).
                hasHostAndPort("s3.amazonaws.com").
                hasPathPrefix("bucket");

        assertThat(parse("s3://s3-eu-west-1.amazonaws.com/bucket")).
                hasHostAndPort("s3-eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://s3-eu-west-1.amazonaws.com/bucket/gggg")).
                hasHostAndPort("s3-eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FILE);
    }

    @Test
    public void checkLocalStackUrl() throws FileSystemException {
        assertThat(parse("s3://localhost:4572/bucket")).
                hasHostAndPort("localhost:4572").
                hasPathPrefix("bucket");
    }

    @Test
    public void checkRegionParsing() {
        assertThat(new S3FileNameParser().regionFromHost("s3-eu-west-1.amazonaws.com", "")).isEqualTo("eu-west-1");
        assertThat(new S3FileNameParser().regionFromHost("s3.amazonaws.com", "us-west-1")).isEqualTo("us-west-1");
        assertThat(new S3FileNameParser().regionFromHost("bucket.s3-eu-west-1.amazonaws.com", "us-west-1")).isEqualTo("eu-west-1");
        assertThat(new S3FileNameParser().regionFromHost("s3-eu-west-1.amazonaws.commmmm", "us-west-1")).isEqualTo("us-west-1");
    }

    private S3FileNameAssert parse(String url) throws FileSystemException {
        return (new S3FileNameAssert((new S3FileNameParser()).parseUri(null, null, url)));
    }

    private static class S3FileNameAssert implements AssertDelegateTarget {
        private final S3FileName file;

        S3FileNameAssert(FileName file) {
            this.file = (S3FileName) file;
        }

        S3FileNameAssert hasHostAndPort(String value) {
            assertThat(file.getHostAndPort()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasPathPrefix(String value) {
            assertThat(file.getPathPrefix()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasType(FileType type) {
            assertThat(file.getType()).isEqualTo(type);

            return this;
        }
    }
}