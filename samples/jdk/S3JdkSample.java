///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17
//DEPS com.github.abashev:vfs-jdk:17.0.1
//DEPS org.slf4j:slf4j-nop:2.0.18

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * The smallest useful application on top of {@code com.github.abashev:vfs-jdk}: an S3 bucket is
 * opened as a {@link java.nio.file.FileSystem}, and the object is written and read back through
 * plain {@link Files} calls.
 *
 * <p>Run it against the MinIO container from {@code ../mise.toml}:
 *
 * <pre>
 * mise trust
 * mise run demo:jdk
 * </pre>
 */
public class S3JdkSample {

    public static void main(String[] args) throws IOException {
        // The canonical URL scheme: the bucket is the host and the rest of the configuration rides
        // in the query. Against real AWS the query is usually just `?region=…`, or nothing at all.
        // Credentials are never part of a URL — they travel in the env map below, and without one
        // the AWS SDK default chain is used.
        var bucket = URI.create("s3://" + env("BUCKET", "vfs-s3-sample") + "?region="
                + env("AWS_REGION", "us-east-1") + "&endpoint=" + env("S3_ENDPOINT", "http://localhost:9000"));
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                env("AWS_ACCESS_KEY_ID", "minioadmin"), env("AWS_SECRET_ACCESS_KEY", "minioadmin")));

        try (var fs = FileSystems.newFileSystem(bucket, Map.of("credentialsProvider", credentials))) {
            // From here on nothing is S3-specific — this is the java.nio.file API you already know.
            var file = fs.getPath("/jdk/hello.txt");

            Files.writeString(file, "Hello from java.nio.file!\n", UTF_8);
            System.out.println("wrote     " + file.toUri() + " (" + Files.size(file) + " bytes)");

            System.out.println("read back " + Files.readString(file, UTF_8).strip());
        }
    }

    private static String env(String name, String fallback) {
        var value = System.getenv(name);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
