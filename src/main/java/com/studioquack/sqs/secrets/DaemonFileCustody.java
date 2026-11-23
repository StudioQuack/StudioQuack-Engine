package com.studioquack.sqs.secrets;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * Single wiring point that hands out purpose-bound {@link DaemonBoundSealedStore} instances for
 * the three on-disk daemon credentials routed through daemon-bound sealed custody. Every store
 * shares the host {@link OsSecretManager} (never null in the app context —
 * {@code OsSecretManagerConfig} falls back to an unavailable null-object) and the env-scoped
 * {@link SecretStoreNamespace}.
 */
@Component
public class DaemonFileCustody {

    /** Purpose tag (OS account + AAD) for the hybrid-enrolment IKM blob. */
    public static final String PURPOSE_KEM_IKM = "kem-ikm";
    /** Purpose tag (OS account + AAD) for the mTLS identity private-key blob. */
    public static final String PURPOSE_MTLS_IDENTITY_KEY = "mtls-identity-key";
    /** Purpose tag (OS account + AAD) for the enrolment-bearer blob. */
    public static final String PURPOSE_ENROLLMENT_BEARER = "enrollment-bearer";
    /** Purpose tag for the :8443 listener EC P-256 private key. */
    public static final String PURPOSE_NET_LISTENER_KEY = "net-listener-key";

    private final OsSecretManager osSecrets;
    private final SecretStoreNamespace namespace;
    private final SqsChannelProperties channel;

    public DaemonFileCustody(OsSecretManager osSecrets,
                             SecretStoreNamespace namespace,
                             SqsChannelProperties channel) {
        this.osSecrets = osSecrets;
        this.namespace = namespace;
        this.channel = channel;
    }

    /** True when the host OS secret manager can hold the per-purpose sealing keys. */
    public boolean custodyAvailable() {
        return osSecrets != null && osSecrets.available();
    }

    /** A store for an explicit purpose + file (the general escape hatch). */
    public DaemonBoundSealedStore forPurpose(String purpose, Path file) {
        return new DaemonBoundSealedStore(osSecrets, namespace,
                channel == null ? "" : channel.getEnvId(), purpose, file);
    }

    /** Custody store for {@code <channelDataDir>/kem/ikm.b64}. */
    public DaemonBoundSealedStore kemIkm(Path channelDataDir) {
        return forPurpose(PURPOSE_KEM_IKM, channelDataDir.resolve("kem").resolve("ikm.b64"));
    }

    /** Custody store for the configured channel data dir's {@code kem/ikm.b64}. */
    public DaemonBoundSealedStore kemIkm() {
        return kemIkm(Path.of(channel.getDataDir()));
    }

    /** Custody store for {@code <channelDataDir>/mtls/identity.key}. */
    public DaemonBoundSealedStore mtlsIdentityKey(Path channelDataDir) {
        return forPurpose(PURPOSE_MTLS_IDENTITY_KEY, channelDataDir.resolve("mtls").resolve("identity.key"));
    }

    /** Custody store for the configured channel data dir's {@code mtls/identity.key}. */
    public DaemonBoundSealedStore mtlsIdentityKey() {
        return mtlsIdentityKey(Path.of(channel.getDataDir()));
    }

    /** Custody store for {@code <channelDataDir>/enrollment/bearer.bin}. */
    public DaemonBoundSealedStore enrollmentBearer(Path channelDataDir) {
        return forPurpose(PURPOSE_ENROLLMENT_BEARER,
                channelDataDir.resolve("enrollment").resolve("bearer.bin"));
    }

    /** Custody store for the configured channel data dir's {@code enrollment/bearer.bin}. */
    public DaemonBoundSealedStore enrollmentBearer() {
        return enrollmentBearer(Path.of(channel.getDataDir()));
    }

    /** Custody store for {@code <channelDataDir>/net/listener.key}. */
    public DaemonBoundSealedStore netListenerKey(Path channelDataDir) {
        return forPurpose(PURPOSE_NET_LISTENER_KEY, channelDataDir.resolve("net").resolve("listener.key"));
    }

    public DaemonBoundSealedStore netListenerKey() {
        return netListenerKey(Path.of(channel.getDataDir()));
    }

    /**
     * Null-safe kemIkm custody resolution for call sites that hold this bean in a field-injected
     * {@code ObjectProvider}.
     */
    public static DaemonBoundSealedStore resolveKemIkm(
            org.springframework.beans.factory.ObjectProvider<DaemonFileCustody> custody,
            SqsChannelProperties channel) {
        DaemonFileCustody c = custody == null ? null : custody.getIfAvailable();
        return c == null ? null : c.kemIkm(Path.of(channel.getDataDir()));
    }

    /**
     * Builds the kemIkm custody store straight from the parts, for components that already inject
     * {@link OsSecretManager} + {@link SecretStoreNamespace} (e.g. the secret command handlers) and
     * should not grow a field just for this.
     */
    public static DaemonBoundSealedStore kemIkmStore(OsSecretManager osSecrets,
                                                     SecretStoreNamespace namespace,
                                                     String envId,
                                                     Path channelDataDir) {
        return new DaemonBoundSealedStore(osSecrets, namespace, envId, PURPOSE_KEM_IKM,
                channelDataDir.resolve("kem").resolve("ikm.b64"));
    }
}
