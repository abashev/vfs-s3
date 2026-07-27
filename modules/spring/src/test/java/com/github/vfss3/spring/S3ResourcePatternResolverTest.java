package com.github.vfss3.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;

final class S3ResourcePatternResolverTest {

    private FakeS3Client s3;
    private S3ResourcePatternResolver resolver;

    @BeforeEach
    void setUp() {
        // A page size of 2 forces multi-page listings, exercising pagination handling.
        s3 = new FakeS3Client(2);
        s3.putBytes("data/a.json", "a".getBytes(UTF_8));
        s3.putBytes("data/b.json", "b".getBytes(UTF_8));
        s3.putBytes("data/notes.txt", "n".getBytes(UTF_8));
        s3.putBytes("data/sub/", new byte[0]);
        s3.putBytes("data/sub/c.json", "c".getBytes(UTF_8));
        s3.putBytes("data/sub/deep/d.json", "d".getBytes(UTF_8));
        s3.putBytes("other/e.json", "e".getBytes(UTF_8));
        resolver = new S3ResourcePatternResolver(s3);
    }

    @Test
    void singleStarStaysWithinOneSegment() throws IOException {
        var resources = resolver.getResources("s3://bucket/data/*.json");
        assertArrayEquals(new String[] {"data/a.json", "data/b.json"}, keys(resources));
    }

    @Test
    void doubleStarCrossesSegments() throws IOException {
        var resources = resolver.getResources("s3://bucket/data/**/*.json");
        assertArrayEquals(
                new String[] {"data/a.json", "data/b.json", "data/sub/c.json", "data/sub/deep/d.json"},
                keys(resources));
    }

    @Test
    void folderMarkersAreExcluded() throws IOException {
        var resources = resolver.getResources("s3://bucket/data/**");
        assertArrayEquals(
                new String[] {"data/a.json", "data/b.json", "data/notes.txt", "data/sub/c.json", "data/sub/deep/d.json"
                },
                keys(resources));
    }

    @Test
    void leadingWildcardListsTheWholeBucket() throws IOException {
        var resources = resolver.getResources("s3://bucket/**/*.json");
        assertArrayEquals(
                new String[] {"data/a.json", "data/b.json", "data/sub/c.json", "data/sub/deep/d.json", "other/e.json"},
                keys(resources));
    }

    @Test
    void zeroMatchesYieldAnEmptyArray() throws IOException {
        assertEquals(0, resolver.getResources("s3://bucket/data/*.xml").length);
    }

    @Test
    void nonPatternLocationResolvesToASingleResource() throws IOException {
        var resources = resolver.getResources("s3://bucket/data/a.json");
        assertEquals(1, resources.length);
        assertInstanceOf(S3Resource.class, resources[0]);
        assertEquals("data/a.json", ((S3Resource) resources[0]).getKey());
    }

    @Test
    void bucketWildcardsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolver.getResources("s3://*/data/a.json"));
    }

    @Test
    void nonS3PatternsDelegateToSpring() throws IOException {
        assertEquals(0, resolver.getResources("classpath*:vfs-s3-does-not-exist/*.nothing").length);
    }

    @Test
    void matchedResourcesAreReadable() throws IOException {
        var resources = resolver.getResources("s3://bucket/data/*.json");
        try (var in = resources[0].getInputStream()) {
            assertEquals("a", new String(in.readAllBytes(), UTF_8));
        }
    }

    private static String[] keys(Resource[] resources) {
        return Arrays.stream(resources)
                .map(resource -> ((S3Resource) resource).getKey())
                .toArray(String[]::new);
    }
}
