package com.github.vfss3.spring;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.lang.Nullable;

/**
 * Internal parsing and validation for {@code s3://} location strings, shared by {@link
 * S3ResourceLoader}, {@link S3ResourcePatternResolver} and {@link S3Resource}.
 *
 * <p>Implements the canonical URI contract this module shares with {@code modules/jdk} (see
 * {@code docs/adr/006-jdk-module-s3-uri-contract.md}): {@code
 * s3://<bucket>[/<key>][?region=…&endpoint=…]}. Credentials are never accepted from a location
 * (userinfo is rejected), and an unrecognized query parameter is rejected fail-fast — a silently
 * ignored typo like {@code ?reigon=} would otherwise surface later as a cryptic signing error.
 *
 * <p>The key part of a location is treated as a raw, already-decoded string — spaces and other
 * special characters are taken literally, exactly as S3 stores them in object keys.
 */
final class S3Uris {

    static final String SCHEME = "s3";
    static final String SCHEME_PREFIX = SCHEME + "://";

    /** Query key for the signing region, e.g. {@code "eu-central-1"} — same name as the jdk module. */
    static final String QUERY_REGION = "region";

    /** Query key for the endpoint override URI, e.g. {@code "http://localhost:9000"}. */
    static final String QUERY_ENDPOINT = "endpoint";

    private S3Uris() {}

    /** A parsed {@code s3://} location: bucket, raw key (no leading slash) and validated query. */
    record S3Location(String bucket, String key, Map<String, String> query) {}

    /** Whether the location starts with {@code s3://} (case-insensitive). */
    static boolean isS3Location(@Nullable String location) {
        return location != null && location.regionMatches(true, 0, SCHEME_PREFIX, 0, SCHEME_PREFIX.length());
    }

    /**
     * Parses a raw {@code s3://bucket/key?region=…&endpoint=…} location string. The first
     * {@code ?} always starts the query component — see the class javadoc for the grammar.
     *
     * @throws IllegalArgumentException on a missing bucket, userinfo credentials, or an
     *     unrecognized query parameter
     */
    static S3Location parse(String location) {
        if (!isS3Location(location)) {
            throw new IllegalArgumentException("Not an s3:// location: " + location);
        }
        var rest = location.substring(SCHEME_PREFIX.length());
        var queryIdx = rest.indexOf('?');
        var head = queryIdx < 0 ? rest : rest.substring(0, queryIdx);
        var query = queryIdx < 0 ? Map.<String, String>of() : parseQuery(rest.substring(queryIdx + 1), location);
        if (head.indexOf('@') >= 0) {
            throw new IllegalArgumentException("Credentials must not be encoded in an s3:// location — configure an"
                    + " AwsCredentialsProvider on the loader instead: " + location);
        }
        var slashIdx = head.indexOf('/');
        var bucket = slashIdx < 0 ? head : head.substring(0, slashIdx);
        var key = slashIdx < 0 ? "" : head.substring(slashIdx + 1);
        if (bucket.isEmpty()) {
            throw new IllegalArgumentException(
                    "No bucket in s3:// location (expected s3://<bucket>[/<key>]): " + location);
        }
        return new S3Location(bucket, key, query);
    }

    /**
     * Validates an already-built {@code s3://} URI and splits it into bucket and key. Any query
     * component is ignored — a URI carries identity, not configuration.
     *
     * @throws IllegalArgumentException on a wrong scheme, a missing bucket, or userinfo
     *     credentials
     */
    static S3Location parse(URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("URI scheme must be '" + SCHEME + "': " + uri);
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Credentials must not be encoded in an s3:// URI: " + uri);
        }
        var bucket = uri.getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("No bucket in s3:// URI (expected s3://<bucket>[/<key>]): " + uri);
        }
        return new S3Location(bucket, key(uri), Map.of());
    }

    private static Map<String, String> parseQuery(String rawQuery, String location) {
        var params = new HashMap<String, String>();
        for (var pair : rawQuery.split("&")) {
            var eq = pair.indexOf('=');
            var name = eq < 0 ? pair : pair.substring(0, eq);
            if (!name.equals(QUERY_REGION) && !name.equals(QUERY_ENDPOINT)) {
                throw new IllegalArgumentException("Unrecognized s3:// query parameter '" + name + "' — only "
                        + QUERY_REGION + " and " + QUERY_ENDPOINT + " are supported: " + location);
            }
            params.put(name, eq < 0 ? "" : pair.substring(eq + 1));
        }
        return params;
    }

    /**
     * Builds a query-free, correctly-encoded {@code s3://} URI from a decoded bucket and key.
     * The multi-argument {@link URI} constructor percent-encodes special characters (spaces, …)
     * in the path while {@link URI#getPath()} decodes them back.
     */
    static URI toUri(String bucket, String key) {
        try {
            return new URI(SCHEME, bucket, key.isEmpty() ? "" : "/" + key, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid S3 location: bucket '" + bucket + "', key '" + key + "'", e);
        }
    }

    /** The object key of an {@code s3://} URI — its decoded path without the leading slash. */
    static String key(URI uri) {
        var path = uri.getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
