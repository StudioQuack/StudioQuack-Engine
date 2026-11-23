package com.studioquack.sqs.release;

/** A downloaded release manifest failed the {@link ReleaseSignatureVerifier} check and must not be used. */
public class ReleaseSignatureException extends RuntimeException {
    public ReleaseSignatureException(String message) {
        super(message);
    }
}
