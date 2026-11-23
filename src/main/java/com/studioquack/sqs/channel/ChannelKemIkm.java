package com.studioquack.sqs.channel;

import org.springframework.stereotype.Component;

/**
 * Holds the live envelope IKM used to verify BE→daemon command MACs: the hybrid transcript, or the
 * {@code SHA-256(bearer)} fallback — whichever enrolment settled on. Installed by {@link SqsChannelBootstrap}
 * after enrolment; until then (boot window) no command is dispatched, and a registry built without this holder
 * is the offline test shape.
 */
@Component
public class ChannelKemIkm {

    private volatile byte[] ikm;
    private volatile String envId;

    public void install(String envId, byte[] ikm) {
        this.envId = envId;
        this.ikm = ikm == null || ikm.length == 0 ? null : ikm.clone();
    }

    public byte[] ikmOrNull() {
        byte[] current = ikm;
        return current == null ? null : current.clone();
    }

    public String envId() {
        return envId;
    }
}
