package com.studioquack.sqs.sandbox.helperchan;

import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;

import java.util.List;
import java.util.Optional;

/**
 * The ONE {@code env=} resolution + outbound authorization pre-check both cross-env caller legs
 * share: {@code studioquack.execute} ({@link HelperExecuteService}) and {@code studioquack.llm.*}
 * ({@link HelperRequestRouter}). Keeping it in one place is what makes the two legs answer
 * identically for the same target: an unknown name is always {@code Error_UnknownEnvironment}
 * here, never a silent fallback to a local run.
 */
final class CrossEnvPeerResolver {

    /** Cap on the resolvable-peer listing embedded in an {@code Error_UnknownEnvironment} message. */
    private static final int MAX_LISTED_PEERS = 20;

    private CrossEnvPeerResolver() {
    }

    /**
     * Resolves an SDK {@code env=} value to the granted peer to dial.
     *
     * @param env The raw {@code env}/{@code envId} value the script passed.
     * @param selfEnvId This daemon's own environment id (from the job execution context).
     * @param ownEnvName This daemon's own environment name (handshake-delivered; nullable).
     */
    static Optional<GrantedPeerDirectory.GrantedPeer> resolvePeer(
            String env, String selfEnvId, String ownEnvName,
            GrantedPeerDirectory directory, CrossEnvAuthorizationClient authorizationClient,
            String scope) {
        if (env == null || env.isBlank()) {
            return Optional.empty();
        }
        String target = env.trim();
        // Naming THIS environment (by id or by name) is the local path, exactly as if omitted.
        if (target.equals(selfEnvId) || (ownEnvName != null && !ownEnvName.isBlank()
                && target.equals(ownEnvName))) {
            return Optional.empty();
        }
        if (directory == null) {
            throw new CrossEnvCallException(CrossEnvCallException.ERROR_AUTHORIZATION_UNAVAILABLE,
                    "cross-env " + scope + " to '" + target + "' cannot be placed: this daemon holds "
                            + "no granted-peer directory (the secure channel is not wired) — refusing "
                            + "rather than falling back to a local run (fail closed)");
        }
        GrantedPeerDirectory.GrantedPeer peer = directory.find(target)
                .orElseThrow(() -> new CrossEnvCallException(
                        CrossEnvCallException.ERROR_UNKNOWN_ENVIRONMENT,
                        "unknown environment '" + target + "': not this environment and not a "
                                + "granted sibling of it. Resolvable environments (id or name): "
                                + describePeers(directory.peers())));
        if (hasScope(peer, scope)) {
            return Optional.of(peer);
        }
        // Outbound scope miss — ask the control plane once before denying: the PEER is the path env ("may
        // MY env call it for scope?"), this daemon's own env is the caller.
        if (authorizationClient == null) {
            throw new CrossEnvCallException(CrossEnvCallException.ERROR_AUTHORIZATION_UNAVAILABLE,
                    "cross-env " + scope + " to '" + target + "' has no pushed grant and this daemon "
                            + "holds no authorization client to ask sq-backend — refusing (fail closed)");
        }
        CrossEnvAuthorizationClient.Outcome outcome =
                authorizationClient.authorize(peer.envId(), selfEnvId, scope);
        return switch (outcome) {
            case ALLOWED -> Optional.of(peer);
            case DENIED -> throw new CrossEnvCallException(CrossEnvCallException.ERROR_NOT_GRANTED,
                    "cross-env " + scope + " not granted: caller env '" + selfEnvId + "' has no '"
                            + scope + "' grant on target env '" + peer.envId() + "' ('" + peer.name()
                            + "') — grant it on the target env's Settings → Cross-env access");
            case UNAVAILABLE -> throw new CrossEnvCallException(
                    CrossEnvCallException.ERROR_AUTHORIZATION_UNAVAILABLE,
                    "cross-env authorization unavailable: no pushed grant for caller env '" + selfEnvId
                            + "' scope '" + scope + "' on target env '" + peer.envId() + "' ('"
                            + peer.name() + "'), and sq-backend could not be reached to authorize — "
                            + "this is a DENY, not a pass");
        };
    }

    /** The pushed scopes cover the call: an exact scope grant or the {@code *} wildcard. */
    private static boolean hasScope(GrantedPeerDirectory.GrantedPeer peer, String scope) {
        List<String> scopes = peer.scopes();
        return scopes != null && (scopes.contains(scope) || scopes.contains("*"));
    }

    /**
     * Shape-only peer listing for an unknown-environment message: {@code name <id>} pairs, bounded
     * — never overlay addresses, never more than {@link #MAX_LISTED_PEERS} entries.
     */
    private static String describePeers(List<GrantedPeerDirectory.GrantedPeer> peers) {
        if (peers == null || peers.isEmpty()) {
            return "(none — no granted peers known; the tunnel may be down or no cross-env grant "
                    + "exists yet)";
        }
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        for (GrantedPeerDirectory.GrantedPeer p : peers) {
            if (shown >= MAX_LISTED_PEERS) {
                sb.append(", … and ").append(peers.size() - shown).append(" more");
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            sb.append(p.name() == null || p.name().isBlank() ? "(unnamed)" : p.name())
                    .append(" <").append(p.envId()).append('>');
            shown++;
        }
        return sb.toString();
    }
}
