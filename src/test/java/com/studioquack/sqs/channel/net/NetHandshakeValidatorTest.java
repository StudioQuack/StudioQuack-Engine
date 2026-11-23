package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every handshake field is bounded before it can reach sq-net argv, TLS trust or iptables. */
class NetHandshakeValidatorTest {

    private static final String PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    private static NetHandshakeResult good() {
        return new NetHandshakeResult(
                "cell-1", "https://control.example.com:443/", "mesh-auth-test-123-xyz",
                "env-64f1c2a3b4d5e6f708091a2b",
                List.of(new HubAddress("gw-1", "203.0.113.10", 5005),
                        new HubAddress("gw-2", "hub-2.example.com", 5005)),
                Instant.now().plusSeconds(3600), "mesh.token.value", "AAAA",
                List.of(new GrantedPeerDirectory.GrantedPeer("64f1c2a3b4d5e6f708091a2c", "sibling", List.of("execute", "llm"))),
                PEM, PEM, "prod",
                List.of(new NetworkRuleStore.Rule("64f1c2a3b4d5e6f708091a2c", 5432)),
                List.of(new NetworkRuleStore.OutboundRule("64f1c2a3b4d5e6f708091a2c", 8443)));
    }

    private static NetHandshakeResult with(String controlUrl, String authKey, String hostname,
                                           List<HubAddress> hubs) {
        NetHandshakeResult g = good();
        return new NetHandshakeResult(g.cellId(), controlUrl, authKey, hostname, hubs, g.keyExpiresAt(),
                g.meshToken(), g.meshVerificationKey(), g.meshPeers(), g.meshCaCertPem(),
                g.listenerCertPem(), g.envName(), g.networkRules(), g.outboundNetworkRules());
    }

    private static NetHandshakeException refused(NetHandshakeResult r) {
        NetHandshakeException ex = assertThrows(NetHandshakeException.class,
                () -> NetHandshakeValidator.validate(r));
        assertEquals(NetHandshakeValidator.ERROR_MALFORMED, ex.errorCode());
        return ex;
    }

    @Test
    void well_formed_response_passes() {
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(good()));
    }

    @Test
    void controlUrl_must_be_http_or_https_with_host() {
        NetHandshakeResult g = good();
        refused(with("file:///etc/passwd", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("javascript:alert(1)", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("https://user:pw@control.example.com/", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("https://control.example.com/\nPostUp=evil", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("http://10.0.0.5:8080/headscale", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("http://192.0.2.10:8443/", g.authKey(), g.hostname(), g.hubAddresses()));
        refused(with("http://203.0.113.9:8443/", g.authKey(), g.hostname(), g.hubAddresses()));
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(
                with("http://localhost:8080", g.authKey(), g.hostname(), g.hubAddresses())));
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(
                with("http://127.0.0.1:8080", g.authKey(), g.hostname(), g.hubAddresses())));
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(
                with("https://dev-net.studioquack.com/", g.authKey(), g.hostname(), g.hubAddresses())));
    }

    @Test
    void authKey_shape_is_bounded() {
        NetHandshakeResult g = good();
        refused(with(g.controlUrl(), "mesh-key\nrm -rf /", g.hostname(), g.hubAddresses()));
        refused(with(g.controlUrl(), "key with spaces", g.hostname(), g.hubAddresses()));
        refused(with(g.controlUrl(), "short", g.hostname(), g.hubAddresses()));
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(
                with(g.controlUrl(), null, g.hostname(), g.hubAddresses())));
    }

    @Test
    void hostname_must_be_a_dns_name() {
        NetHandshakeResult g = good();
        refused(with(g.controlUrl(), g.authKey(), "env-1\r\n--advertise-routes=0.0.0.0/0", g.hubAddresses()));
        refused(with(g.controlUrl(), g.authKey(), "env_1 --reset", g.hubAddresses()));
        refused(with(g.controlUrl(), g.authKey(), "-leading-hyphen", g.hubAddresses()));
        refused(with(g.controlUrl(), g.authKey(), "", g.hubAddresses()));
        assertTrue(NetHandshakeValidator.validHostname("env-64f1c2a3b4d5e6f708091a2b.cell.example"));
    }

    @Test
    void hub_entries_must_be_ip_literals_or_host_names() {
        NetHandshakeResult g = good();
        refused(with(g.controlUrl(), g.authKey(), g.hostname(),
                List.of(new HubAddress("gw", "203.0.113.10; curl evil", 5005))));
        refused(with(g.controlUrl(), g.authKey(), g.hostname(),
                List.of(new HubAddress("gw", "", 5005))));
        refused(with(g.controlUrl(), g.authKey(), g.hostname(),
                List.of(new HubAddress("gw/../x", "203.0.113.10", 5005))));
        assertDoesNotThrow(() -> NetHandshakeValidator.validate(with(g.controlUrl(), g.authKey(), g.hostname(),
                List.of(new HubAddress(null, "2001:db8::10", 5005)))));
    }

    @Test
    void peer_and_rule_identifiers_are_opaque_tokens() {
        NetHandshakeResult g = good();
        NetHandshakeResult badPeer = new NetHandshakeResult(g.cellId(), g.controlUrl(), g.authKey(),
                g.hostname(), g.hubAddresses(), g.keyExpiresAt(), g.meshToken(), g.meshVerificationKey(),
                List.of(new GrantedPeerDirectory.GrantedPeer("../../etc", "x", List.of("execute"))),
                g.meshCaCertPem(), g.listenerCertPem(), g.envName(), g.networkRules(), g.outboundNetworkRules());
        refused(badPeer);

        NetHandshakeResult badScope = new NetHandshakeResult(g.cellId(), g.controlUrl(), g.authKey(),
                g.hostname(), g.hubAddresses(), g.keyExpiresAt(), g.meshToken(), g.meshVerificationKey(),
                List.of(new GrantedPeerDirectory.GrantedPeer("64f1c2a3b4d5e6f708091a2c", "x", List.of("EXECUTE ALL"))),
                g.meshCaCertPem(), g.listenerCertPem(), g.envName(), g.networkRules(), g.outboundNetworkRules());
        refused(badScope);

        NetHandshakeResult badRule = new NetHandshakeResult(g.cellId(), g.controlUrl(), g.authKey(),
                g.hostname(), g.hubAddresses(), g.keyExpiresAt(), g.meshToken(), g.meshVerificationKey(),
                g.meshPeers(), g.meshCaCertPem(), g.listenerCertPem(), g.envName(),
                List.of(new NetworkRuleStore.Rule("0.0.0.0/0", 22)), g.outboundNetworkRules());
        refused(badRule);

        NetHandshakeResult badOutbound = new NetHandshakeResult(g.cellId(), g.controlUrl(), g.authKey(),
                g.hostname(), g.hubAddresses(), g.keyExpiresAt(), g.meshToken(), g.meshVerificationKey(),
                g.meshPeers(), g.meshCaCertPem(), g.listenerCertPem(), g.envName(), g.networkRules(),
                List.of(new NetworkRuleStore.OutboundRule("64f1c2a3b4d5e6f708091a2c", 70000)));
        refused(badOutbound);
    }

    @Test
    void certificate_fields_must_be_pem() {
        NetHandshakeResult g = good();
        NetHandshakeResult bad = new NetHandshakeResult(g.cellId(), g.controlUrl(), g.authKey(),
                g.hostname(), g.hubAddresses(), g.keyExpiresAt(), g.meshToken(), g.meshVerificationKey(),
                g.meshPeers(), "not a cert", g.listenerCertPem(), g.envName(), g.networkRules(),
                g.outboundNetworkRules());
        refused(bad);
    }
}
