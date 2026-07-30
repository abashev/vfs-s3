///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17
//DEPS com.github.abashev:vfs-spring:17.0.1
//DEPS org.springframework:spring-context:6.2.4
//DEPS org.slf4j:slf4j-nop:2.0.18

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.vfss3.spring.S3ClientConfig;
import com.github.vfss3.spring.S3ProtocolResolver;
import com.github.vfss3.spring.S3ResourceLoader;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.WritableResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * The smallest useful Spring application on top of {@code com.github.abashev:vfs-spring}: an
 * {@code s3://} object is injected into a field with {@code @Value}, written to, and read back.
 *
 * <p>Run it against the MinIO container from {@code mise.toml}:
 *
 * <pre>
 * mise trust
 * mise run demo
 * </pre>
 */
public class S3SpringSample {

    /**
     * The entire integration, from the application's point of view: an {@code s3://} location
     * injected exactly like a {@code classpath:} or {@code file:} one. No AWS SDK type in sight —
     * the bucket only has to exist, the object does not.
     */
    @Value("s3://vfs-s3-sample/spring/hello.txt")
    private WritableResource report;

    private void run() throws IOException {
        try (var out = report.getOutputStream()) {
            out.write("Hello from Spring @Value!\n".getBytes(UTF_8));
        }
        System.out.println("wrote     " + report.getURI() + " (" + report.contentLength() + " bytes)");

        try (var in = report.getInputStream()) {
            System.out.println("read back " + new String(in.readAllBytes(), UTF_8).strip());
        }
    }

    public static void main(String[] args) throws IOException {
        // The only S3-aware lines in the whole application. Against real AWS this is just
        // `new S3ResourceLoader()` — region and credentials then come from the AWS SDK default
        // chains; the settings below point the sample at the local MinIO started by `mise run up`.
        var config = S3ClientConfig.defaults()
                .withRegion(env("AWS_REGION", "us-east-1"))
                .withEndpoint(env("S3_ENDPOINT", "http://localhost:9000"))
                .withCredentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        env("AWS_ACCESS_KEY_ID", "minioadmin"), env("AWS_SECRET_ACCESS_KEY", "minioadmin"))));

        try (var loader = new S3ResourceLoader(config);
                var context = new AnnotationConfigApplicationContext()) {
            // Registering the resolver is what teaches the context the s3:// scheme, so every
            // ambient lookup — @Value, @PropertySource, context.getResource(…) — understands it.
            // It has to happen before refresh(), while beans are still being created.
            context.addProtocolResolver(new S3ProtocolResolver(loader));
            context.register(S3SpringSample.class);
            context.refresh();

            context.getBean(S3SpringSample.class).run();
        }
    }

    private static String env(String name, String fallback) {
        var value = System.getenv(name);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
