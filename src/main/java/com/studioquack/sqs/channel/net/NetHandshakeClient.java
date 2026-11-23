package com.studioquack.sqs.channel.net;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Handshake v2: {@code POST …/net/handshake} with the enrolment bearer. No overlay IP, no PSK, no KEM
 * ciphertext — the agent owns the tunnel.
 */
@Slf4j
public class NetHandshakeClient {

    public static final String PATH = "/net/handshake";

    private final String environmentBase;
    private final String envId;
    private final String bearer;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final NetAgentClient agent;
    private final ListenerKeyStore listenerKeys;
    private final MeshIdentity meshIdentity;
    private final GrantedPeerDirectory grantedPeerDirectory;
    private final OverlayPeerDirectoryAdapter overlayPeerDirectory;
    private final NetworkRuleStore networkRuleStore;
    private volatile com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore crossEnvGrantStore;
    private final Duration timeout = Duration.ofSeconds(20);

    private volatile NetHandshakeResult last;
    private volatile Instant lastControlUrlKnown;
    /**
     * Instant the held mesh token should be rotated (80 % of its signed life). The token TTL (default 35 min) is
     * FAR shorter than the node-key life (days) the watchdog's healthy-path refresh was keyed on — without this, a
     * healthy overlay stopped re-handshaking and every cross-env call began failing
     * {@code Error_PeerRejectedIdentity} ~35 min after bring-up.
     */
    private volatile Instant meshTokenRefreshAt;

    public NetHandshakeClient(String environmentBase, String envId, String bearer,
                              ChannelHttpClients httpClients, ObjectMapper mapper,
                              NetAgentClient agent, ListenerKeyStore listenerKeys,
                              MeshIdentity meshIdentity, GrantedPeerDirectory grantedPeerDirectory,
                              OverlayPeerDirectoryAdapter overlayPeerDirectory,
                              NetworkRuleStore networkRuleStore) {
        this.environmentBase = environmentBase;
        this.envId = envId;
        this.bearer = bearer;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.agent = agent;
        this.listenerKeys = listenerKeys;
        this.meshIdentity = meshIdentity;
        this.grantedPeerDirectory = grantedPeerDirectory;
        this.overlayPeerDirectory = overlayPeerDirectory;
        this.networkRuleStore = networkRuleStore;
    }

    public NetHandshakeResult last() {
        return last;
    }

    /**
     * POST handshake v2, apply mesh identity / grants / NAC, optionally {@code sq-net up}.
     */
    public synchronized NetHandshakeResult handshake() throws Exception {
        String hostname = PeerAddressResolver.hostnameFor(envId);
        String agentVersion = agent == null ? "" : nullToEmpty(agent.version());
        String nodeKey = null;
        // Reported to the BE so it can tell "node registered in netmgmt AND the local
        // agent can still use that registration" (skip the mint) apart from "registered
        // but the local session is gone" (must re-issue — see bringAgentUp).
        Boolean agentSessionUp = null;
        try {
            if (agent != null) {
                NetAgentStatus st = agent.status();
                if (st != null && st.publicKey() != null && !st.publicKey().isBlank()) {
                    nodeKey = st.publicKey();
                }
                agentSessionUp = st != null && st.running();
            }
        } catch (Exception e) {
            // UNKNOWN, not down: a status failure (LocalAPI ACL, agent restarting) says
            // nothing about the registration, and "down" makes the BE mint a key that
            // retires a possibly-working node. null keeps the BE's netmgmt-only decision.
            agentSessionUp = null;
            log.debug("channel/net: agent status before up ({})", e.toString());
        }
        String csr = listenerKeys == null ? null : listenerKeys.csrPem(hostname);
        HandshakeRequest req = new HandshakeRequest(agentVersion, hostname, nodeKey, csr, agentSessionUp);
        HttpResponse<String> resp = ChannelHttp.postJson(
                httpClients.get(), environmentBase + PATH, bearer,
                mapper.writeValueAsString(req), timeout);
        int status = resp.statusCode();
        if (status == 404) {
            throw new NetHandshakeException(status, NetHandshakeException.ERROR_ENV_NOT_FOUND,
                    "net handshake: " + NetHandshakeException.ERROR_ENV_NOT_FOUND
                            + " body=" + ChannelHttp.trim(resp.body()), null);
        }
        if (status == 503) {
            throw new NetHandshakeException(status, NetHandshakeException.ERROR_NETWORK_UNAVAILABLE,
                    "net handshake: " + NetHandshakeException.ERROR_NETWORK_UNAVAILABLE
                            + " body=" + ChannelHttp.trim(resp.body()),
                    parseRetryAfter(resp));
        }
        if (status == 409) {
            throw new NetHandshakeException(status, NetHandshakeException.ERROR_NO_CELL_CAPACITY,
                    "net handshake: " + NetHandshakeException.ERROR_NO_CELL_CAPACITY
                            + " body=" + ChannelHttp.trim(resp.body()),
                    parseRetryAfter(resp));
        }
        if (!ChannelHttp.isOk(status)) {
            throw new NetHandshakeException(status, errorCodeOf(resp.body()),
                    "net handshake: status " + status + " body=" + ChannelHttp.trim(resp.body()),
                    parseRetryAfter(resp));
        }
        HandshakeResponse hr = mapper.readValue(resp.body(), HandshakeResponse.class);
        if (blank(hr.controlUrl()) && blank(hr.hostname())) {
            throw new NetHandshakeException(status, "Error_MalformedHandshake",
                    "net handshake: response missing controlUrl/hostname", null);
        }
        String hn = blank(hr.hostname()) ? hostname : hr.hostname();
        Instant expires = parseInstant(hr.keyExpiresAt());
        List<HubAddress> hubs = hr.hubAddresses() == null ? List.of() : hr.hubAddresses();
        NetHandshakeResult result = new NetHandshakeResult(
                hr.cellId(), hr.controlUrl(), hr.authKey(), hn, hubs, expires,
                hr.meshToken(), hr.meshVerificationKey(),
                hr.meshPeers() == null ? List.of() : hr.meshPeers(),
                firstNonBlank(hr.meshCaCertPem(), hr.meshCaCert()),
                firstNonBlank(hr.listenerCertPem(), hr.listenerCert()),
                hr.envName(),
                hr.networkRules(), hr.outboundNetworkRules(), hr.crossEnvInbound());
        // Nothing from the response reaches sq-net argv, TLS trust, the rule store or the
        // peer directory until every field has been parsed and bounded. A refusal keeps the
        // previous handshake's state in force.
        NetHandshakeValidator.validate(result);
        apply(result);
        bringAgentUp(result, hn);
        this.last = result;
        this.lastControlUrlKnown = Instant.now();
        this.meshTokenRefreshAt = meshTokenRefreshAt(result.meshToken());
        log.info("channel/net: handshake ok env={} cell={} hubs={} peers={} authKey={}",
                envId, result.cellId(), result.hubAddresses().size(),
                result.meshPeers().size(), result.hasAuthKey() ? "issued" : "null");
        return result;
    }

    /**
     * The store every handshake refreshes with the backend's inbound grant set.
     */
    public void setCrossEnvGrantStore(com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore store) {
        this.crossEnvGrantStore = store;
    }

    /**
     * {@code true} when a re-handshake should run now: never handshook, the mesh token is at 80 % of its signed
     * life (the common case — minutes), or 80 % of the node-key life has elapsed (days).
     */
    public boolean shouldRefresh() {
        NetHandshakeResult r = last;
        if (r == null) {
            return true;
        }
        Instant tokenRefresh = meshTokenRefreshAt;
        if (tokenRefresh != null && !Instant.now().isBefore(tokenRefresh)) {
            return true;
        }
        if (r.keyExpiresAt() == null) {
            return false;
        }
        Instant exp = r.keyExpiresAt();
        Instant start = lastControlUrlKnown == null ? Instant.now().minusSeconds(60) : lastControlUrlKnown;
        long total = Math.max(1L, exp.getEpochSecond() - start.getEpochSecond());
        long elapsed = Instant.now().getEpochSecond() - start.getEpochSecond();
        return elapsed >= (long) (total * 0.8d);
    }

    /**
     * Reads {@code iat}/{@code exp} out of the compact-JWS mesh token and returns the instant at 80 % of its
     * signed life. {@code null} when the token is absent or not a parseable JWS — the refresh decision then falls
     * back to the node-key schedule (and a peer 401 still triggers the reactive re-handshake).
     */
    private Instant meshTokenRefreshAt(String token) {
        if (blank(token)) {
            return null;
        }
        try {
            String[] parts = token.trim().split("\\.");
            if (parts.length != 3) {
                return null;
            }
            JsonNode payload = mapper.readTree(
                    java.util.Base64.getUrlDecoder().decode(parts[1]));
            long iat = payload.path("iat").asLong(0);
            long exp = payload.path("exp").asLong(0);
            if (iat <= 0 || exp <= iat) {
                return null;
            }
            return Instant.ofEpochSecond(iat + (long) ((exp - iat) * 0.8d));
        } catch (Exception ex) {
            return null;
        }
    }

    private void apply(NetHandshakeResult result) throws Exception {
        if (meshIdentity != null) {
            meshIdentity.update(result.meshToken(), result.meshVerificationKey(), result.envName());
        }
        if (grantedPeerDirectory != null) {
            grantedPeerDirectory.replace(grantedPeerDirectory.primaryKey(), result.meshPeers());
        }
        if (overlayPeerDirectory != null) {
            overlayPeerDirectory.replaceGrantedEnvIds(result.meshPeers());
        }
        if (networkRuleStore != null) {
            networkRuleStore.replaceAll(result.networkRules(), result.outboundNetworkRules());
        }
        // The inbound set starts empty at every daemon start and was otherwise refilled only by a
        // reload-config push (enrolment / rule change) — a rebooted engine refused its granted
        // callers until somebody edited a rule. Every handshake now restores it wholesale.
        com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore inbound = this.crossEnvGrantStore;
        if (inbound != null && result.crossEnvInbound() != null) {
            inbound.replace(result.crossEnvInbound());
        }
        if (listenerKeys != null && result.listenerCertPem() != null && !result.listenerCertPem().isBlank()) {
            X509Certificate leaf = MeshTls.parseFirstCert(result.listenerCertPem());
            listenerKeys.installLeaf(leaf);
        }
    }

    private void bringAgentUp(NetHandshakeResult result, String hostname) throws Exception {
        if (agent == null || blank(result.controlUrl())) {
            return;
        }
        if (!result.hasAuthKey()) {
            final NetAgentStatus st;
            try {
                st = agent.status();
            } catch (Exception e) {
                // No key to log in with AND the agent is not answering — nothing here can
                // succeed. Fail the handshake so the watchdog retries on its fast ladder
                // instead of parking until the mesh-token refresh (~half an hour).
                throw new NetHandshakeException(-1, "Error_NetAgentUnreachable",
                        "channel/net: authKey null and the agent is not answering status: " + e, null);
            }
            if (st != null && st.running()) {
                log.info("channel/net: authKey null and agent Running — skipping up");
                return;
            }
            // `sq-net up` WITHOUT a key only helps an agent that still holds a login
            // session (Stopped → resume).




            if (st == null || !"Stopped".equalsIgnoreCase(st.backendState())) {
                log.warn("channel/net: BE issued no auth key and the agent has no resumable "
                        + "session (state={}) — NOT running `sq-net up` keyless; the next "
                        + "handshake re-asks with agentSessionUp=false",
                        st == null ? "unknown" : st.backendState());
                return;
            }
        }
        agent.up(result.controlUrl(), result.authKey(), hostname);
    }

    private static Integer parseRetryAfter(HttpResponse<String> resp) {
        try {
            String v = resp.headers().firstValue("Retry-After").orElse(null);
            if (v == null || v.isBlank()) {
                return null;
            }
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Instant parseInstant(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(raw.trim());
        } catch (Exception e) {
            log.debug("channel/net: handshake instant parse failed ({})", e.toString());
            return null;
        }
    }

    private static String errorCodeOf(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        try {
            JsonNode n = new ObjectMapper().readTree(body);
            if (n.has("errorCode")) {
                return n.get("errorCode").asText("");
            }
            if (n.has("code")) {
                return n.get("code").asText("");
            }
        } catch (Exception e) {
            log.debug("channel/net: handshake error body was not JSON ({})", e.toString());
        }
        return "";
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) {
            return a;
        }
        return b;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record HandshakeRequest(String agentVersion, String hostname, String nodeKey,
                                   String listenerCsrPem, Boolean agentSessionUp) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HandshakeResponse(
            String cellId,
            String controlUrl,
            String authKey,
            String hostname,
            List<HubAddress> hubAddresses,
            String keyExpiresAt,
            String meshToken,
            String meshVerificationKey,
            List<GrantedPeerDirectory.GrantedPeer> meshPeers,
            String meshCaCertPem,
            String meshCaCert,
            String listenerCertPem,
            String listenerCert,
            String envName,
            List<NetworkRuleStore.Rule> networkRules,
            List<NetworkRuleStore.OutboundRule> outboundNetworkRules,
            List<com.studioquack.sqs.entitlement.CrossEnvInboundGrant> crossEnvInbound) {
    }

    /**
     * Narrow seam so handshake can refresh the overlay grant directory without depending on the concrete
     * {@code OverlayPeerDirectory} constructor graph.
     */
    @FunctionalInterface
    public interface OverlayPeerDirectoryAdapter {
        void replaceGrantedEnvIds(List<GrantedPeerDirectory.GrantedPeer> peers);
    }
}
