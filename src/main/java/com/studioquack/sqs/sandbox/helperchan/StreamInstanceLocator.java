package com.studioquack.sqs.sandbox.helperchan;

import java.util.Map;
import java.util.Optional;

/**
 * Resolves a stream-through target node to a daemon-dialable HTTP endpoint. Production
 * uses the live stateful instance for the node's OCI image (same {@code (env, imageRef)}
 * key as dispatch); tests substitute an in-process listener.
 *
 * <p>Never starts an instance: a miss means the frame is dropped.
 */
public interface StreamInstanceLocator {

    /**
     * @param host Address the daemon JVM can connect to (container bridge IP, or loopback in test.
     * @param port In-instance app port ({@code SQ_APP_PORT}, default 8080).
     * @param basePath Invoke path ({@code SQ_APP_PATH}, e.g.
     * @param imageRef Image the live instance is running, for helper-job provenance.
     * @param containerId Live container id for in-flight accounting; null in tests.
     */
    record Location(String host, int port, String basePath, String imageRef, String containerId) {
        public Location(String host, int port, String basePath, String imageRef) {
            this(host, port, basePath, imageRef, null);
        }

        /** {@code http://host:port/invoke/stream} (basePath already includes the leading slash). */
        public String streamUrl() {
            String path = basePath == null || basePath.isBlank() ? "/invoke" : basePath;
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            return "http://" + host + ":" + port + path + "/stream";
        }
    }

    /**
     * Live instance for {@code targetNode} in {@code envId}, or empty when none is running
     * or the node has no image ref. Must refuse a container owned by another environment.
     */
    Optional<Location> locate(String envId, Map<String, Object> targetNode);
}
