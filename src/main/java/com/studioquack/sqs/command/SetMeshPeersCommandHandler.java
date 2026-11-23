package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.MeshPeerSetChangedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code set-mesh-peers}: applies a backend-pushed refresh of this daemon's OUTBOUND granted-peer directory (
 * {@link GrantedPeerDirectory}) between handshakes. Why it exists: the directory is otherwise fed only by the
 * WireGuard handshake's {@code meshPeers}, so a cross-env grant created between handshakes left the caller
 * daemon resolving {@code env=} against a stale (often empty) view — the call failed with
 * {@code Error_UnknownEnvironment} until the next reconnect.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SetMeshPeersCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "set-mesh-peers";

    private final ObjectMapper objectMapper;
    private final GrantedPeerDirectory grantedPeerDirectory;
    private final ApplicationEventPublisher eventPublisher;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String payload = command == null ? null : command.payload();
        if (payload == null || payload.isBlank()) {
            return DaemonCommandOutcome.failed("set-mesh-peers: missing payload");
        }
        List<GrantedPeerDirectory.GrantedPeer> peers = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode array = root == null ? null : root.get("meshPeers");
            if (array == null || !array.isArray()) {
                return DaemonCommandOutcome.failed("set-mesh-peers: payload carries no meshPeers array");
            }
            for (JsonNode node : array) {
                GrantedPeerDirectory.GrantedPeer peer =
                        objectMapper.treeToValue(node, GrantedPeerDirectory.GrantedPeer.class);
                if (peer != null && peer.envId() != null && !peer.envId().isBlank()) {
                    peers.add(peer);
                }
            }
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("set-mesh-peers: malformed payload: "
                    + (parse.getMessage() == null ? parse.toString() : parse.getMessage()));
        }
        boolean addressesChanged = grantedPeerDirectory.replace(grantedPeerDirectory.primaryKey(), peers);
        if (addressesChanged) {

            // Env carries a NEW overlay IP, and allowed-ips/host routes only refresh at handshake
            // ask the channel layer to re-sync the interface now instead
            // of leaving calls to connect-timeout for up to one rotation cadence.
            eventPublisher.publishEvent(
                    new MeshPeerSetChangedEvent(this, grantedPeerDirectory.primaryKey()));
        }
        return DaemonCommandOutcome.ok("{\"peers\":" + peers.size() + "}");
    }
}
