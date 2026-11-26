package com.studioquack.sqs.channel.net;

/**
 * Handshake v2 failure. {@link #envNotFound()} is terminal ( self-disable); {@link #networkUnavailable()}
 * stays on public HTTPS with bounded backoff; {@link #noCellCapacity()} retries later.
 */
public class NetHandshakeException extends RuntimeException {

    public static final String ERROR_ENV_NOT_FOUND = "Error_EnvNotFound";
    public static final String ERROR_NETWORK_UNAVAILABLE = "Error_NetworkUnavailable";
    public static final String ERROR_NO_CELL_CAPACITY = "Error_NoCellCapacity";

    private final int status;
    private final String errorCode;
    private final Integer retryAfterSeconds;

    public NetHandshakeException(int status, String errorCode, String message, Integer retryAfterSeconds) {
        super(message);
        this.status = status;
        this.errorCode = errorCode;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int status() {
        return status;
    }

    public String errorCode() {
        return errorCode;
    }

    public Integer retryAfterSeconds() {
        return retryAfterSeconds;
    }

    public boolean envNotFound() {
        return status == 404 || ERROR_ENV_NOT_FOUND.equals(errorCode);
    }

    public boolean networkUnavailable() {
        return status == 503 || ERROR_NETWORK_UNAVAILABLE.equals(errorCode);
    }

    public boolean noCellCapacity() {
        return status == 409 || ERROR_NO_CELL_CAPACITY.equals(errorCode);
    }

    public boolean unauthorized() {
        return status == 401;
    }
}
