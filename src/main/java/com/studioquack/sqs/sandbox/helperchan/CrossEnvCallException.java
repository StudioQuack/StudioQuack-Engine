package com.studioquack.sqs.sandbox.helperchan;

/**
 * A cross-environment call failure carrying one of the error codes. Thrown by the caller-side legs
 * ({@link HelperExecuteService} for {@code studioquack.execute}, {@link HelperRequestRouter}'s llm
 * leg for {@code studioquack.llm.*}) so the helper channel can surface a DISTINGUISHABLE reason —
 * the live-execution animation shows a red node with the real cause instead of an opaque "execute
 * failed".
 */
public class CrossEnvCallException extends RuntimeException {

    /** {@code env} names nothing the caller can see (not self, not a granted sibling). */
    public static final String ERROR_UNKNOWN_ENVIRONMENT = "Error_UnknownEnvironment";
    /** Resolvable target, but no {@code (caller, target, scope)} grant — caller-side deny. */
    public static final String ERROR_NOT_GRANTED = "Error_CrossEnvNotGranted";
    /** Granted peer could not be dialed on the overlay (connect/timeout at dial time). */
    public static final String ERROR_PEER_UNREACHABLE = "Error_PeerUnreachable";
    /** No mesh identity to present, or the peer refused the presented token (401). */
    public static final String ERROR_PEER_REJECTED_IDENTITY = "Error_PeerRejectedIdentity";
    /** The PEER environment has no AI provider configured (its 503 provider_not_configured). */
    public static final String ERROR_PEER_PROVIDER_NOT_CONFIGURED = "Error_PeerProviderNotConfigured";
    /** Grant miss and the control plane could not answer — explicitly a DENY, never a pass. */
    public static final String ERROR_AUTHORIZATION_UNAVAILABLE = "Error_AuthorizationUnavailable";

    private final String errorCode;

    public CrossEnvCallException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public CrossEnvCallException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /** The code this failure carries (never {@code null}). */
    public String errorCode() {
        return errorCode;
    }
}
