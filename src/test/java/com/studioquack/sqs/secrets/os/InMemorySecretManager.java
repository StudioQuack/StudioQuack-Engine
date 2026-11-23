package com.studioquack.sqs.secrets.os;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-lifetime, in-memory {@link OsSecretManager} fake for unit tests that need to exercise
 * the {@link OsSecretManager} contract off any real platform store. Test-only; never shipped in
 * production artifacts.
 */
public class InMemorySecretManager implements OsSecretManager {

    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    private static String key(String service, String account) {
        return service + "\u0000" + account;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public String name() {
        return "in-memory(test)";
    }

    @Override
    public Optional<String> get(String service, String account) {
        return Optional.ofNullable(store.get(key(service, account)));
    }

    @Override
    public void put(String service, String account, String secret) {
        store.put(key(service, account), secret);
    }

    @Override
    public boolean delete(String service, String account) {
        return store.remove(key(service, account)) != null;
    }
}
