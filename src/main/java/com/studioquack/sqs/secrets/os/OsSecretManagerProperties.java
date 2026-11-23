package com.studioquack.sqs.secrets.os;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Operator-facing configuration for OS secret-manager key custody ({@code sqs.secrets.os.*}).
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "sqs.secrets.os")
public class OsSecretManagerProperties {

    /**
     * When {@code true} (the default) the daemon seals {@code execution_context} at rest with an
     * OS-managed key and resolves {@code $secretRef} from the OS store. When {@code false}, custody
     * degrades to the safe pre-rollout posture (plaintext-disabled context crypto + no-op secret
     * resolution) — useful for a bring-up host with no reachable platform store.
     */
    private boolean enabled = true;

    /**
     * Which backend to use: {@code auto} (detect the platform store, default), or an explicit pin —
     * {@code macos}, {@code secret-tool}, {@code keyctl}, {@code machine-file}, {@code windows}, or
     * {@code off}. An explicit pin that turns out unavailable degrades gracefully (the daemon does not
     * fail to boot).
     */
    private String provider = "auto";

    /**
     * Namespacing prefix for entries this daemon owns in the shared OS store, so multiple apps (and
     * multiple SQS data dirs) don't collide. Entry services are derived as
     * {@code <servicePrefix>.<purpose>} (e.g. {@code com.studioquack.sqs.execution-context}).
     */
    private String servicePrefix = "com.studioquack.sqs";

    /**
     * Account/instance discriminator within {@link #servicePrefix}. Distinct values let one host run
     * multiple isolated SQS instances with independent keys (defaults to a single instance).
     */
    private String account = "default";

    /**
     * Filesystem home for file-backed stores ({@code machine-file} and Windows DPAPI blobs).
     * Mirrors the engine data dir; bound from {@code SQS_DATA_DIR} in the shipping config.
     */
    private String dataDir = "./data";
}
