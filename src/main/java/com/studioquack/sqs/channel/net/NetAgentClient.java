package com.studioquack.sqs.channel.net;

/**
 * Operator of the bundled mesh agent ({@code sq-net-agent} / {@code sq-net} CLI). The daemon never programs
 * WireGuard itself: every data-plane action is a CLI invocation against the agent socket the installer granted
 * this user.
 */
public interface NetAgentClient {

    /**
     * Joins the cell. {@code authKey} may be {@code null} when the node is already registered and
     * {@link #status()} reports {@code Running} against the same {@code controlUrl} — callers skip {@code up} in
     * that case.
     */
    void up(String controlUrl, String authKey, String hostname) throws Exception;

    /**
     * Tears the session down (best-effort).
     */
    void down();

    /**
     * Forgets the stored node identity (uninstall / lock).
     */
    void logout();

    /**
     * {@code sq-net version} (empty when the CLI cannot be asked).
     */
    String version();

    /**
     * {@code sq-net status --json} snapshot.
     */
    NetAgentStatus status() throws Exception;

    /**
     * Backend label for skip-reason / logs ({@code cli} when this adapter is live).
     */
    default String backend() {
        return "cli";
    }
}
