package com.studioquack.sqs.secrets.os;

/**
 * Thrown by {@link OsSecretManager#put} (and internal helpers) when the OS secret-manager backend
 * fails a write/custody operation. The message is built from the CLI's non-secret diagnostics only
 * — it never carries secret material.
 */
public class OsSecretException extends RuntimeException {

    public OsSecretException(String message) {
        super(message);
    }

    public OsSecretException(String message, Throwable cause) {
        super(message, cause);
    }
}
