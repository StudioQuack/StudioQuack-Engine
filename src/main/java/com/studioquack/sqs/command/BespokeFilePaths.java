package com.studioquack.sqs.command;

import com.studioquack.sqs.sandbox.stateful.StatefulInstance;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Shared validation and path helpers for bespoke-file put/delete/list command handlers. Every constraint is
 * re-checked on the daemon; the BE's controller validation is never trusted alone.
 */
public final class BespokeFilePaths {

    /**
     * {@code templateId} folder segment under {@code /data/sq-bespoke/<templateId>/}.
     */
    public static final Pattern TEMPLATE_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._-]{1,64}$");

    /**
     * Container root for all bespoke handler files.
     */
    public static final String BESPOKE_ROOT = "/data/sq-bespoke";

    /**
     * Instance id reserved for the platform Docling sidecar (case-insensitive match).
     */
    private static final String PLATFORM_DOCLING_INSTANCE_ID = "docling";

    /**
     * Matched as a substring of the normalised image ref (lowercase, {@code /} → {@code -}), mirroring
     * {@code StatefulInstanceServiceImpl.isPlatformManagedRef}.
     */
    private static final String PLATFORM_DOCLING_IMAGE_MARKER = "docling-serve";

    private BespokeFilePaths() {
    }

    /**
     * Trim and require a -safe template id, or throw {@link IllegalArgumentException}.
     */
    public static String requireTemplateId(String templateId) {
        if (templateId == null) {
            throw new IllegalArgumentException("templateId must not be blank");
        }
        String tid = templateId.trim();
        if (tid.isEmpty() || !TEMPLATE_ID_PATTERN.matcher(tid).matches()) {
            throw new IllegalArgumentException(
                    "templateId must match ^[A-Za-z0-9._-]{1,64}$");
        }
        return tid;
    }

    /**
     * Require a basename-only file name. Refuses blank, {@code /}, {@code \}, and {@code.} outright — never strips
     * path components to "fix" a bad name.
     */
    public static String requireBasename(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("bespoke file name must not be blank");
        }
        // Do not strip: reject the original string if it contains forbidden sequences.
        if (name.contains("/") || name.contains("\\") || name.contains("..")) {
            throw new IllegalArgumentException(
                    "bespoke file name must be a basename (no '/', '\\', or '..')");
        }
        String n = name.trim();
        if (n.isEmpty()) {
            throw new IllegalArgumentException("bespoke file name must not be blank");
        }
        // Re-check after trim in case whitespace-only slipped past isBlank edge cases.
        if (n.contains("/") || n.contains("\\") || n.contains("..")) {
            throw new IllegalArgumentException(
                    "bespoke file name must be a basename (no '/', '\\', or '..')");
        }
        return n;
    }

    /**
     * {@code /data/sq-bespoke/<templateId>}
     */
    public static String containerDir(String templateId) {
        return BESPOKE_ROOT + "/" + templateId;
    }

    /**
     * {@code /data/sq-bespoke/<templateId>/<name>}
     */
    public static String containerPath(String templateId, String name) {
        return containerDir(templateId) + "/" + name;
    }

    /**
     * True when the instance is platform-managed Docling and must refuse bespoke-file ops. Matches instance id
     * {@code docling} (ignore case) or an image ref whose normalised lowercase form (with {@code /} → {@code -})
     * contains {@code docling-serve}.
     */
    public static boolean isPlatformManaged(StatefulInstance inst) {
        if (inst == null) {
            return false;
        }
        String instanceId = inst.instanceId();
        if (instanceId != null
                && PLATFORM_DOCLING_INSTANCE_ID.equalsIgnoreCase(instanceId.trim())) {
            return true;
        }
        String imageRef = inst.imageRef();
        if (imageRef == null || imageRef.isBlank()) {
            return false;
        }
        String normalised = imageRef.toLowerCase(Locale.ROOT).replace('/', '-');
        return normalised.contains(PLATFORM_DOCLING_IMAGE_MARKER);
    }

    /**
     * POSIX single-quote escaping (same idiom as {@code ContainerBootstrap}): wrap in {@code '} and replace
     * embedded {@code '} with {@code '\''}.
     */
    public static String shSingleQuote(String s) {
        if (s == null) {
            return "''";
        }
        return "'" + s.replace("'", "'\\''") + "'";
    }
}
