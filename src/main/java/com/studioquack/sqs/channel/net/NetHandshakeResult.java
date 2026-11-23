package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;

import java.time.Instant;
import java.util.List;

/**
 * Parsed handshake v2 response. {@code authKey} is null when the node is already registered and the key is
 * unexpired — callers skip {@code sq-net up} if status is already Running against the same {@code controlUrl}.
 */
public record NetHandshakeResult(
        String cellId,
        String controlUrl,
        String authKey,
        String hostname,
        List<HubAddress> hubAddresses,
        Instant keyExpiresAt,
        String meshToken,
        String meshVerificationKey,
        List<GrantedPeerDirectory.GrantedPeer> meshPeers,
        String meshCaCertPem,
        String listenerCertPem,
        String envName,
        List<NetworkRuleStore.Rule> networkRules,
        List<NetworkRuleStore.OutboundRule> outboundNetworkRules,
        /**
         * Who may call THIS engine, the same wholesale set {@code reload-config} carries. {@code null} when the
         * backend predates the field — the inbound store is then left exactly as it was.
         */
        List<com.studioquack.sqs.entitlement.CrossEnvInboundGrant> crossEnvInbound) {

    public NetHandshakeResult {
        hubAddresses = hubAddresses == null ? List.of() : List.copyOf(hubAddresses);
        meshPeers = meshPeers == null ? List.of() : List.copyOf(meshPeers);
        networkRules = networkRules == null ? List.of() : List.copyOf(networkRules);
        outboundNetworkRules = outboundNetworkRules == null ? List.of() : List.copyOf(outboundNetworkRules);
        crossEnvInbound = crossEnvInbound == null ? null : List.copyOf(crossEnvInbound);
    }

    /**
     * Pre-inbound shape (tests and callers that do not carry inbound grants).
     */
    public NetHandshakeResult(String cellId, String controlUrl, String authKey, String hostname,
                              List<HubAddress> hubAddresses, Instant keyExpiresAt, String meshToken,
                              String meshVerificationKey, List<GrantedPeerDirectory.GrantedPeer> meshPeers,
                              String meshCaCertPem, String listenerCertPem, String envName,
                              List<NetworkRuleStore.Rule> networkRules,
                              List<NetworkRuleStore.OutboundRule> outboundNetworkRules) {
        this(cellId, controlUrl, authKey, hostname, hubAddresses, keyExpiresAt, meshToken,
                meshVerificationKey, meshPeers, meshCaCertPem, listenerCertPem, envName,
                networkRules, outboundNetworkRules, null);
    }

    public boolean hasAuthKey() {
        return authKey != null && !authKey.isBlank();
    }
}
