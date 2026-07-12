package com.github.vfss3.commonsvfs.remote;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Translates the remote {@code BASE_URL} into the commons-vfs URL dialect this module's production
 * {@code S3FileNameParser} understands.
 *
 * <p>ADR-006 (in {@code modules/jdk}) made the jdk URI — {@code s3://<bucket>?region=…&endpoint=…},
 * bucket in the host, region/endpoint in the query — the canonical CI {@code BASE_URL}. commons-vfs
 * instead encodes the endpoint and region in the <em>host</em>, so the remote suite rebuilds a URL
 * in that dialect before handing it to VFS. This is a <strong>test-only</strong> adapter:
 * commons-vfs production code stays untouched; it is the mirror image of the jdk module's own
 * {@code RemoteEndpoint}, which consumes the same {@code BASE_URL} natively.
 *
 * <p>The rebuilt URL is always path-style ({@code s3://<host>/<bucket>/}):
 *
 * <ul>
 *   <li>no {@code endpoint} (AWS-native) &rarr; host {@code s3.<region>.amazonaws.com}, so
 *       {@code S3FileNameParser} recovers the region from the host;
 *   <li>an {@code endpoint} &rarr; that endpoint's {@code host[:port]} (Yandex, DigitalOcean,
 *       a local emulator, …), which {@code S3FileNameParser} matches by its own host patterns or
 *       treats as a custom endpoint.
 * </ul>
 */
final class RemoteEndpoint {

    private static final String DEFAULT_AWS_REGION = "us-east-1";

    private RemoteEndpoint() {}

    /**
     * @param urlTemplate a jdk-dialect {@code s3://<bucket>?region=…&endpoint=…} template with a
     *     {@code %s} placeholder in the bucket for the per-run bucket-name token
     * @param token substituted for the {@code %s} placeholder
     * @return a commons-vfs path-style URL addressing the same bucket/region/endpoint
     */
    static String toCommonsVfsUrl(String urlTemplate, String token) {
        var uri = URI.create(String.format(urlTemplate, token));

        var bucket = uri.getHost();
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException(
                    "BASE_URL has no bucket in the host position (expected s3://<bucket>?…): " + urlTemplate);
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException(
                    "BASE_URL must not carry credentials in userinfo — supply them via the AWS SDK "
                            + "credentials chain instead: " + urlTemplate);
        }

        var params = queryParams(uri, urlTemplate);
        var region = params.get("region");
        var endpoint = params.get("endpoint");

        String host;
        if (endpoint == null || endpoint.isBlank()) {
            var signingRegion = (region == null || region.isBlank()) ? DEFAULT_AWS_REGION : region;
            host = "s3." + signingRegion + ".amazonaws.com";
        } else {
            var endpointUri = URI.create(endpoint);
            host = endpointUri.getHost();
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("BASE_URL endpoint has no host: " + endpoint);
            }
            if (endpointUri.getPort() != -1) {
                host = host + ":" + endpointUri.getPort();
            }
        }

        return "s3://" + host + "/" + bucket + "/";
    }

    /** Parses the recognized {@code region} / {@code endpoint} query parameters, rejecting others. */
    private static Map<String, String> queryParams(URI uri, String urlTemplate) {
        var params = new HashMap<String, String>();
        var query = uri.getQuery();
        if (query == null || query.isBlank()) {
            return params;
        }
        for (var pair : query.split("&")) {
            var eq = pair.indexOf('=');
            var key = eq < 0 ? pair : pair.substring(0, eq);
            if (!key.equals("region") && !key.equals("endpoint")) {
                throw new IllegalArgumentException("Unrecognized BASE_URL query parameter '" + key
                        + "' — only region and endpoint are supported: " + urlTemplate);
            }
            params.put(key, eq < 0 ? "" : pair.substring(eq + 1));
        }
        return params;
    }
}
