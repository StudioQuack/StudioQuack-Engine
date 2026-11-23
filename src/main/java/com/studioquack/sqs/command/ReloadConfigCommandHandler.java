package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.docling.DoclingContainer;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * {@code reload-config}: the daemon side of the server-resolved entitlement push. Auto-discovered by
 * {@code DaemonCommandRegistry} like every other {@link DaemonCommandHandler}, so no transport change is
 * needed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReloadConfigCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type; matches the control plane.
     */
    public static final String TYPE = "reload-config";

    private final ObjectMapper objectMapper;
    private final EntitlementsHolder entitlementsHolder;
    private final DoclingContainer doclingContainer;
    /**
     * The inbound cross-env grant set, replaced wholesale from the snapshot's {@code crossEnvInbound} on every
     * payload-bearing reload (a no-payload reload leaves it as-is).
     */
    private final com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore crossEnvGrantStore;

    /**
     * The on-miss authorize cache, evicted when a new grant set lands below so a revoked caller cannot be
     * re-authorized from a stale ALLOW for the rest of the backend TTL. Optional: the client bean only exists when
     * the channel is enabled, and field injection keeps the constructor (and its tests) unchanged.
     */
    @Autowired(required = false)
    private com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient crossEnvAuthorizationClient;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String payload = command == null ? null : command.payload();

        // No payload → benign operator reload. Do NOT mutate entitlements; the
        // daemon simply re-reads / re-logs its current config.
        if (payload == null || payload.isBlank()) {
            EnvironmentEntitlements current = entitlementsHolder.current();
            log.info("reload-config (no payload): operator reload; current logRetentionDays={} (unlimited={})",
                    current.logRetentionDays(), current.retentionUnlimited());
            return DaemonCommandOutcome.ok(ack("reloaded", current));
        }

        // Payload present → it is the server-resolved entitlement snapshot.
        EnvironmentEntitlements snapshot;
        try {
            snapshot = objectMapper.readValue(payload, EnvironmentEntitlements.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("reload-config: malformed entitlement payload: " + message(parse));
        }
        if (snapshot == null) {
            return DaemonCommandOutcome.failed("reload-config: empty entitlement payload");
        }

        // Freshness guard: the BE stamps every command with issuedAt at enqueue time, and
        // delivery order is NOT enqueue order across the two transports — a snapshot that
        // sat in the durable queue while the daemon was offline can land after a newer one
        // (the replay that stopped the Docling side-car right after the operator enabled it).
        // A stale snapshot must not touch entitlements, the grant store, or the Docling
        // reconcile: every effect it carries is older than what is already applied.
        if (!entitlementsHolder.applyIfFresh(snapshot, parseIssuedAt(command.issuedAt()))) {
            log.warn("reload-config: STALE snapshot ignored (issuedAt={}) — a newer push is "
                    + "already applied; entitlements, grants and Docling state left untouched",
                    command.issuedAt());
            return DaemonCommandOutcome.ok(ack("stale-ignored", entitlementsHolder.current()));
        }
        // The same snapshot carries the inbound cross-env grant set; apply it
        // wholesale so a grant/revoke takes effect with this push, not at the next handshake.
        // The daemon caches and applies — the control plane remains the only policy author.
        crossEnvGrantStore.replace(snapshot.crossEnvInbound());
        // A revoked grant must not survive in the on-miss authorize cache: the new set is
        // authoritative, so every cached decision ends with it.
        if (crossEnvAuthorizationClient != null) {
            crossEnvAuthorizationClient.evictAll();
        }
        log.info("reload-config applied: logRetentionDays={} (unlimited={}) monitoring={} webhookRelay={} docling={} webhookMessageEvents={} crossEnvInboundGrants={}",
                snapshot.logRetentionDays(), snapshot.retentionUnlimited(),
                snapshot.monitoringEnabled(), snapshot.webhookRelay(), snapshot.doclingEnabled(),
                snapshot.webhookMessageEventsEnabled(), snapshot.crossEnvInbound().size());
        // Bring the Docling stateful instance up/down to match the entitlement that was JUST applied,
        // so the Engine Settings toggle takes effect immediately rather than at the next workflow run
        // that happens to need it. Async (see DoclingContainer#reconcile) — never blocks this command.
        doclingContainer.reconcile(snapshot.doclingEnabled());
        return DaemonCommandOutcome.ok(ack("applied", snapshot));
    }

    /**
     * Small JSON ack shipped back to the BE (stored in its recent-results ring).
     */
    private String ack(String reload, EnvironmentEntitlements entitlements) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("reload", reload);
            node.put("logRetentionDays", entitlements.logRetentionDays());
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            // Ack is best-effort; never fail the command over the ack shape.
            return "{\"reload\":\"" + reload + "\"}";
        }
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Parses the BE enqueue stamp. Blank/unparseable → {@code null}, which fails the freshness guard OPEN: a
     * malformed stamp must never brick entitlement delivery.
     */
    private static java.time.Instant parseIssuedAt(String issuedAt) {
        if (issuedAt == null || issuedAt.isBlank()) {
            return null;
        }
        try {
            return java.time.Instant.parse(issuedAt.trim());
        } catch (Exception bad) {
            return null;
        }
    }
}
