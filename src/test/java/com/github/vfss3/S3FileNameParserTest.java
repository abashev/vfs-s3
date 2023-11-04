package com.github.vfss3;

import com.github.vfss3.parser.S3FileNameParser;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.assertj.core.api.AssertDelegateTarget;
import org.testng.annotations.Test;

import static org.apache.commons.vfs2.FileType.FOLDER;
import static org.apache.commons.vfs2.FileType.IMAGINARY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class S3FileNameParserTest {
    @Test
    public void checkHostedStyleUrl() throws FileSystemException {
        assertThat(parse("s3://bucket.s3.amazonaws.com")).
                hasEndpoint("s3.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://bucket.s3.eu-west-1.amazonaws.com")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://bucket.s3.eu-west-1.amazonaws.com/some_file")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY);

        assertThat(parse("s3://bucket.s3.eu-west-1.amazonaws.com/some file")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net/some_file")).
                hasEndpoint("storage.yandexcloud.net").
                hasPathPrefix(null).
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net/some file")).
                hasEndpoint("storage.yandexcloud.net").
                hasPathPrefix(null).
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests.oss-cn-beijing.aliyuncs.com/some_file")).
                hasEndpoint("oss-cn-beijing.aliyuncs.com").
                hasPathPrefix(null).
                hasSigningRegion("cn-beijing").
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests.oss-cn-beijing.aliyuncs.com/some file")).
                hasEndpoint("oss-cn-beijing.aliyuncs.com").
                hasPathPrefix(null).
                hasSigningRegion("cn-beijing").
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests2.ams3.digitaloceanspaces.com/some file")).
                hasEndpoint("ams3.digitaloceanspaces.com").
                hasPathPrefix(null).
                hasSigningRegion("ams3").
                hasType(IMAGINARY);

        assertThat(parse("s3://s3-tests.obs.ru-moscow-1.hc.sbercloud.ru/some file")).
                hasEndpoint("obs.ru-moscow-1.hc.sbercloud.ru").
                hasPathPrefix(null).
                hasSigningRegion("ru-moscow-1").
                hasType(IMAGINARY);
    }

    @Test
    public void checkPathStyleUrl() throws FileSystemException {
        assertThat(parse("s3://s3.amazonaws.com/bucket")).
                hasEndpoint("s3.amazonaws.com").
                hasPathPrefix("bucket");

        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/bucket")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/bucket")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/bucket/gggg")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY).
                hasPath("/gggg");

        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/bucket/concurrent/")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY).
                hasPath("/concurrent");

        assertThat(parse("s3://s3.eu-central-1.amazonaws.com/bucket/concurrent/")).
                hasEndpoint("s3.eu-central-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY).
                hasPath("/concurrent");

        assertThat(parse("s3://s3.eu-central-1.amazonaws.com/bucket/conc urrent/")).
                hasEndpoint("s3.eu-central-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(IMAGINARY).
                hasPath("/conc urrent");

        assertThat(parse("s3://frifqsbag2em.compat.objectstorage.eu-frankfurt-1.oraclecloud.com/s3-tests//some file")).
                hasEndpoint("frifqsbag2em.compat.objectstorage.eu-frankfurt-1.oraclecloud.com").
                hasPathPrefix("s3-tests").
                hasSigningRegion("eu-frankfurt-1").
                hasType(IMAGINARY);
    }

    @Test(expectedExceptions = FileSystemException.class)
    public void checkAliyunPathStyleUrl() throws FileSystemException {
        parse("s3://oss-cn-beijing.aliyuncs.com/test-bucket/some-file");
    }

    @Test
    public void checkLocalStackUrl() throws FileSystemException {
        assertThat(parse("s3://localhost:4572/bucket")).
                hasEndpoint("localhost:4572").
                hasPathPrefix("bucket");
    }

    @Test
    public void checkRegionParsing() throws FileSystemException {
        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/bucket")).
                hasSigningRegion("eu-west-1");

        assertThat(parse("s3://s3.amazonaws.com/bucket")).
                hasSigningRegion("us-east-1");

        assertThat(parse("s3://bucket.s3.eu-west-1.amazonaws.com")).
                hasSigningRegion("eu-west-1");

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net")).
                hasSigningRegion("ru-central1");

        assertThat(parse("s3://s3-tests.oss.aliyuncs.com")).
                hasSigningRegion("cn-hangzhou");
    }

    @Test
    public void checkBrokenUrls() throws FileSystemException {
        assertThat(parse("s3://s3.eu-west-1.amazonaws.com/s3-tests//big_file.iso")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("s3-tests").
                hasPath("/big_file.iso");

        assertThat(parse("s3://s3.eu-west-1.amazonaws.com///////s3-tests///////big_file.iso")).
                hasEndpoint("s3.eu-west-1.amazonaws.com").
                hasPathPrefix("s3-tests").
                hasPath("/big_file.iso");

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net////////s3-tests///////big_file.iso")).
                hasEndpoint("storage.yandexcloud.net").
                hasUrlPrefix("s3-tests").
                hasPathPrefix(null).
                hasPath("/s3-tests/big_file.iso");

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net")).
                hasEndpoint("storage.yandexcloud.net").
                hasUrlPrefix("s3-tests").
                hasPathPrefix(null).
                hasPath("/");

        assertThat(parse("s3://s3-tests.storage.yandexcloud.net//")).
                hasEndpoint("storage.yandexcloud.net").
                hasUrlPrefix("s3-tests").
                hasPathPrefix(null).
                hasPath("/");

        assertThat(parse("s3://s3-tests.oss.aliyuncs.com////////s3-tests///////big_file.iso")).
                hasEndpoint("oss.aliyuncs.com").
                hasUrlPrefix(null).
                hasPathPrefix(null).
                hasPath("/s3-tests/big_file.iso");
    }

    @Test(expectedExceptions = FileSystemException.class)
    public void wrongKeys() throws FileSystemException {
        assertThat(parse("s3://access@bucket.s3.amazonaws.com")).
                hasEndpoint("s3.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);
    }

    @Test
    public void checkCredentials() throws FileSystemException {
        assertThat(parse("s3://access:secret@bucket.s3.amazonaws.com")).
                hasEndpoint("s3.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER);

        assertThat(parse("s3://access:secret:cn-north-1@bucket.s3.amazonaws.com")).
                hasEndpoint("s3.cn-north-1.amazonaws.com").
                hasPathPrefix("bucket").
                hasType(FOLDER).
                hasSigningRegion("cn-north-1");
    }

    private S3FileNameAssert parse(String url) throws FileSystemException {
        return (new S3FileNameAssert((new S3FileNameParser()).parseUri(null, null, url)));
    }

    private static class S3FileNameAssert implements AssertDelegateTarget {
        private final S3FileName file;

        S3FileNameAssert(FileName file) {
            this.file = (S3FileName) file;
        }

        S3FileNameAssert hasEndpoint(String value) {
            assertThat(file.getEndpoint()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasPathPrefix(String value) {
            assertThat(file.getPathPrefix()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasUrlPrefix(String value) {
            assertThat(file.getUrlPrefix()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasType(FileType type) {
            assertThat(file.getType()).isEqualTo(type);

            return this;
        }

        S3FileNameAssert hasPath(String value) {
            assertThat(file.getPath()).isEqualTo(value);

            return this;
        }

        S3FileNameAssert hasSigningRegion(String value) {
            assertThat(file.getSigningRegion()).isEqualTo(value);

            return this;
        }

    }
}
