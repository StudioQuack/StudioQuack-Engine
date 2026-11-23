package com.studioquack.common.security.envelope;

/**
 * How the daemon protects the enrolment bearer and hybrid-KEM private keys at rest.
 * Reported on each heartbeat. A higher ordinal is a weaker posture.
 */
public enum BearerProtectionPosture {

    /**
     * Bearer + KEM private keys live in a TPM-2.0-sealed (Linux),
     * Secure-Enclave-backed (macOS), or DPAPI-via-VSC (Windows)
     * keystore. Release of the bytes requires a successful
     * platform-attested unlock at daemon start. Strongest posture
     * — survives a stolen disk image and a host-userland exploit
     * that cannot escalate to the secure element.
     */
    TPM_SEALED,

    /**
     * Bytes live in an OS keychain (macOS Keychain Services,
     * Windows Credential Manager, Linux Secret Service / kwallet)
     * that requires user-authentication at daemon start. Stronger
     * than the file fallback because root cannot read the bytes
     * without an interactive unlock, but weaker than TPM because
     * a compromised user-session can still trick the keychain.
     */
    OS_KEYCHAIN,

    /**
     * Bytes live in a root-owned file on an immutable mount.
     * Root can still read the file; the mount blocks changes between restarts.
     */
    FILE_IMMUTABLE,

    /** Bytes live in a regular file on a mutable filesystem. Weakest stored posture. */
    FILE_PLAIN,

    /**
     * Daemon did not include the {@code bearerProtection} field on
     * its heartbeat (older binary). Treated as
     * {@link #FILE_PLAIN} for severity purposes but flagged
     * separately so operators can target the upgrade.
     */
    UNKNOWN
}
