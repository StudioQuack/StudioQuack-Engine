package com.studioquack.sqs.sandbox.oci;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Daemon-side path and buildId rules for OCI build context materialisation. Mirrors {@code
 * BuildWorkspacePathValidator} without depending on it: SQS vendors its own copy.
 */
public final class OciBuildPathValidator {

    /** UUID-safe buildId charset (letters, digits, dot, underscore, hyphen). */
    private static final Pattern BUILD_ID_SAFE = Pattern.compile("^[A-Za-z0-9._-]+$");

    private OciBuildPathValidator() {
    }

    /**
     * Validate and normalise a single relative path.
     *
     * @return Normalised path ({@code a//b} → {@code a/b}).
     * @throws IllegalArgumentException On any violation.
     */
    public static String normalizeAndValidate(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("OCI build path is required.");
        }
        String path = rawPath.trim();
        if (path.startsWith("/")) {
            throw new IllegalArgumentException("OCI build path must be relative (no leading '/').");
        }
        if (path.indexOf('\\') >= 0) {
            throw new IllegalArgumentException(
                    "OCI build path must use '/' separators, not backslashes.");
        }
        // Collapse repeated slashes; still refuse a trailing '/' (file-keyed tree).
        String[] parts = path.split("/", -1);
        if (parts.length > 0 && parts[parts.length - 1].isEmpty()) {
            throw new IllegalArgumentException(
                    "OCI build path must not contain empty segments (e.g. '//' or trailing '/').");
        }
        List<String> normalised = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (part.isEmpty()) {
                // Mid-path empty from repeated '/' — skip (normalise), do not keep.
                continue;
            }
            if (".".equals(part) || "..".equals(part)) {
                throw new IllegalArgumentException(
                        "OCI build path must not contain '.' or '..' segments.");
            }
            normalised.add(part);
        }
        if (normalised.isEmpty()) {
            throw new IllegalArgumentException("OCI build path is required.");
        }
        return String.join("/", normalised);
    }

    /**
     * Require a non-blank, filesystem-safe buildId (UUID-safe charset; no path separators).
     *
     * @return trimmed buildId
     * @throws IllegalArgumentException when blank or unsafe
     */
    public static String requireBuildId(String buildId) {
        if (buildId == null || buildId.isBlank()) {
            throw new IllegalArgumentException("buildId is required.");
        }
        String id = buildId.trim();
        if (id.indexOf('/') >= 0 || id.indexOf('\\') >= 0 || id.contains("..")) {
            throw new IllegalArgumentException("buildId must not contain path separators or '..'.");
        }
        if (!BUILD_ID_SAFE.matcher(id).matches()) {
            throw new IllegalArgumentException(
                    "buildId must match [A-Za-z0-9._-]+ (UUID-safe).");
        }
        return id;
    }
}
