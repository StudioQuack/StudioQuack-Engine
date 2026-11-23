package com.studioquack.sqs.secrets.os;

import java.util.Optional;

/**
 * Cross-platform SPI over the host's OS secret manager — the trusted, kernel-/OS-backed store that
 * holds key material at rest so the daemon never keeps a long-lived secret in a config file,
 * environment variable, or the local database/WAL. This is the custody primitive both P0 gaps
 * flagged at the foundation gate depend on (/ §4): the {@code execution_context} at-rest AES key (
 * {@link com.studioquack.sqs.execution.crypto.OsKeychainContextKeyProvider});.
 */
public interface OsSecretManager {

    /**
     * @return {@code true} when this manager can actually talk to a working backend on this host
     * (correct OS, CLI present on PATH, probe succeeded). When {@code false}, callers must fall back
     * to their safe default (disabled context crypto / no-op secret resolution) rather than fail.
     */
    boolean available();

    /**
     * @return a short, non-secret label identifying the backing store for the boot banner / audit
     * log (e.g. {@code "macos-keychain"}, {@code "linux-secret-tool"}, {@code "machine-file"},
     * {@code "unavailable"}). Never contains secret material.
     */
    String name();

    /**
     * Reads the secret stored under {@code (service, account)}.
     *
     * @return the plaintext value, or {@link Optional#empty()} when there is no such entry (and,
     * defensively, when a read error occurs — logged, never throwing, so a get-or-create caller can
     * proceed to create).
     */
    Optional<String> get(String service, String account);

    /**
     * Creates or replaces the secret stored under {@code (service, account)}.
     *
     * @throws OsSecretException when the backend rejects/fails the write, so callers can distinguish
     * a genuine custody failure from a benign miss and decide whether to degrade or abort.
     */
    void put(String service, String account, String secret);

    /** Removes the secret stored under {@code (service, account)}. */
    boolean delete(String service, String account);
}
