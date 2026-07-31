///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17
//DEPS com.github.abashev:vfs-commons:17.0.1
//DEPS org.apache.commons:commons-vfs2:2.10.0
//DEPS org.slf4j:slf4j-nop:2.0.18

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.vfss3.commonsvfs.S3FileSystemOptions;
import java.net.URI;
import org.apache.commons.vfs2.VFS;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * The smallest useful application on top of {@code com.github.abashev:vfs-commons}: an S3 object is
 * resolved as an Apache Commons VFS {@code FileObject}, written and read back.
 *
 * <p>Run it against the MinIO container from {@code ../mise.toml}:
 *
 * <pre>
 * mise trust
 * mise run demo:commons-vfs
 * </pre>
 */
public class S3CommonsVfsSample {

    public static void main(String[] args) throws Exception {
        var options = new S3FileSystemOptions();
        options.setCredentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                env("AWS_ACCESS_KEY_ID", "minioadmin"), env("AWS_SECRET_ACCESS_KEY", "minioadmin"))));
        // Both settings exist for local and S3-compatible servers; against real AWS neither is
        // needed — credentials alone (or the AWS SDK default chain) are enough.
        options.setUseHttps(false);
        options.setDisableChunkedEncoding(true);

        // This module keeps the legacy dialect: the endpoint is the host, and the bucket is the
        // first path segment. Against AWS the host carries the region instead, as in
        // s3://my-bucket.s3.eu-central-1.amazonaws.com/commons-vfs/hello.txt
        var endpoint = URI.create(env("S3_ENDPOINT", "http://localhost:9000"));
        var url = "s3://" + endpoint.getHost() + ":" + endpoint.getPort() + "/" + env("BUCKET", "vfs-s3-sample")
                + "/commons-vfs/hello.txt";

        var manager = VFS.getManager();
        var file = manager.resolveFile(url, options.toFileSystemOptions());

        try {
            try (var out = file.getContent().getOutputStream()) {
                out.write("Hello from Commons VFS!\n".getBytes(UTF_8));
            }
            System.out.println(
                    "wrote     " + file.getName().getURI() + " (" + file.getContent().getSize() + " bytes)");

            try (var in = file.getContent().getInputStream()) {
                System.out.println("read back " + new String(in.readAllBytes(), UTF_8).strip());
            }
        } finally {
            // Closing the file system shuts down the transfer threads, so the JVM can exit.
            manager.closeFileSystem(file.getFileSystem());
        }
    }

    private static String env(String name, String fallback) {
        var value = System.getenv(name);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
