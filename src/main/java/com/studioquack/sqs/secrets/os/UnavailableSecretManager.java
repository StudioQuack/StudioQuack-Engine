package com.studioquack.sqs.secrets.os;

import java.util.Optional;

/**
 * The safe null object selected when no OS secret manager is usable on this host and no dev
 * fallback is permitted (a deployment on a host whose platform store the daemon cannot reach).
 * {@link #available} is {@code false}, so consumers degrade to their fail-safe posture.
 */
public class UnavailableSecretManager implements OsSecretManager {

    @Override
    public boolean available() {
        return false;
    }

    @Override
    public String name() {
        return "unavailable";
    }

    @Override
    public Optional<String> get(String service, String account) {
        return Optional.empty();
    }

    @Override
    public void put(String service, String account, String secret) {
        throw new OsSecretException("no OS secret manager is available on this host — cannot persist "
                + "service=" + service + " account=" + account);
    }

    @Override
    public boolean delete(String service, String account) {
        return false;
    }
}
