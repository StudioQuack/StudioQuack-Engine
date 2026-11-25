package com.studioquack.sqs.status;

/**
 * Byte / row ceilings for engine -> control-plane sandbox-log POSTs.
 *
 * <p>A request over the control-plane ceiling returns HTTP 413 ({@code Error_LogTooLarge} /
 * {@code Error_LogBatchTooLarge}). Compliant engines chunk under {@link #MAX_CHUNK_BYTES}
 * so a hydrated BATCH flush never trips that reject; a non-compliant engine surfaces the
 * 413 as a hard log-send failure (no retry).
 */
public final class SandboxLogIngestLimits {

    private SandboxLogIngestLimits() {
    }

    /** Matches control-plane 16 MiB ingest ceiling. */
    public static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;

    /**
     * Per-POST budget the engine aims for. Below {@link #MAX_REQUEST_BYTES} so JSON
     * framing and UTF-8 expansion cannot push Content-Length over the reject ceiling.
     */
    public static final int MAX_CHUNK_BYTES = 12 * 1024 * 1024;

    /** Matches control-plane max batch row count. */
    public static final int MAX_BATCH_ROWS = 200;

    /** Control-plane errorCode on an oversize single-row or batch body. */
    public static final String ERROR_LOG_TOO_LARGE = "Error_LogTooLarge";

    /** Control-plane errorCode on a batch with too many rows. */
    public static final String ERROR_LOG_BATCH_TOO_LARGE = "Error_LogBatchTooLarge";

    /** True when the control plane refused the body as oversize (do not retry). */
    public static boolean isOversizeRefusal(int statusCode, String body) {
        if (statusCode != 413) {
            return false;
        }
        if (body == null || body.isBlank()) {
            return true;
        }
        return body.contains(ERROR_LOG_TOO_LARGE) || body.contains(ERROR_LOG_BATCH_TOO_LARGE);
    }
}
