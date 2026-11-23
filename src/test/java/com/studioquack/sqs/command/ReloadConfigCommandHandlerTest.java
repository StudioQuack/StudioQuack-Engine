package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.docling.DoclingContainer;
import com.studioquack.sqs.entitlement.EntitlementsHolder;
import com.studioquack.sqs.entitlement.EnvironmentEntitlements;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** {@link ReloadConfigCommandHandler}: proves the OPTIONAL-payload contract end-to-end on the daemon side */
class ReloadConfigCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private EntitlementsHolder holder;
    private DoclingContainer doclingContainer;
    private com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore grantStore;
    private ReloadConfigCommandHandler handler;

    @BeforeEach
    void setUp() {
        holder = new EntitlementsHolder(1); // base default floor
        doclingContainer = mock(DoclingContainer.class);
        grantStore = new com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore();
        handler = new ReloadConfigCommandHandler(mapper, holder, doclingContainer, grantStore);
    }

    private DaemonCommand command(String id, String payload) {
        return new DaemonCommand(id, ReloadConfigCommandHandler.TYPE, "2026-07-13T00:00:00Z", payload);
    }

    @Test
    void type_isReloadConfig() {
        assertEquals("reload-config", handler.type());
    }

    @Test
    void snapshotPayload_isAppliedLive_andAckEchoesRetention() throws Exception {
        String payload = "{\"logRetentionDays\":30,\"fixOpsEnabled\":true,"
                + "\"monitoringEnabled\":false,\"webhookRelay\":true,\"doclingEnabled\":false}";

        DaemonCommandOutcome outcome = handler.handle(command("c-apply", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertEquals(30, holder.logRetentionDays(), "retention changed live from 1 -> 30");
        assertTrue(holder.current().webhookRelay());
        assertFalse(holder.current().monitoringEnabled());

        Map<?, ?> ack = mapper.readValue(outcome.output(), Map.class);
        assertEquals("applied", ack.get("reload"));
        assertEquals(30, ((Number) ack.get("logRetentionDays")).intValue());
    }

    @Test
    void unlimitedSnapshot_isApplied() {
        DaemonCommandOutcome outcome = handler.handle(command("c-unlimited",
                "{\"logRetentionDays\":-1,\"fixOpsEnabled\":false,"
                        + "\"monitoringEnabled\":false,\"webhookRelay\":false,\"doclingEnabled\":false}"));

        assertTrue(outcome.succeeded());
        assertEquals(-1, holder.logRetentionDays());
        assertTrue(holder.current().retentionUnlimited());
    }

    @Test
    void malformedPayload_returnsFailed_neverThrows_andHolderUnchanged() {
        DaemonCommandOutcome outcome = handler.handle(command("c-bad", "{ not-json"));

        assertFalse(outcome.succeeded(), "malformed payload must fail");
        assertNotNull(outcome.error(), "failure carries a BE-visible message");
        assertEquals(1, holder.logRetentionDays(), "a bad payload must not mutate the live snapshot");
    }

    @Test
    void noPayload_isBenignReload_ok_andHolderUnchanged() throws Exception {
        // Pre-seed a non-default value via a prior apply; the no-arg reload keeps it.
        holder.apply(new EnvironmentEntitlements(15, false, false, false));

        DaemonCommandOutcome outcome = handler.handle(command("c-noop", null));

        assertTrue(outcome.succeeded());
        assertEquals(15, holder.logRetentionDays(), "no-payload reload must not change entitlements");
        Map<?, ?> ack = mapper.readValue(outcome.output(), Map.class);
        assertEquals("reloaded", ack.get("reload"));
    }

    @Test
    void blankPayload_isTreatedAsNoPayload() {
        DaemonCommandOutcome outcome = handler.handle(command("c-blank", "   "));

        assertTrue(outcome.succeeded());
        assertEquals(1, holder.logRetentionDays());
    }

    @Test
    void snapshotWithApiKeyHashes_isCapturedLive_whenWebhookRelayEntitled() {
        // The The caller additively appends apiKeyHashes to the SAME reload-config snapshot when
        // the webhook relay add-on is entitled. The client must capture BOTH webhookRelay and the
        // hashes.
        String payload = "{\"logRetentionDays\":7,\"fixOpsEnabled\":true,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":true,\"doclingEnabled\":false,"
                + "\"apiKeyHashes\":[\"$2a$10$abcdefghijklmnopqrstuvA\",\"$2a$10$abcdefghijklmnopqrstuvB\"]}";

        DaemonCommandOutcome outcome = handler.handle(command("c-apikeys", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertTrue(holder.webhookRelay(), "webhookRelay is captured from the snapshot");
        assertTrue(holder.current().hasApiKeyHashes(), "the snapshot reports it carries key material");
        assertEquals(List.of("$2a$10$abcdefghijklmnopqrstuvA", "$2a$10$abcdefghijklmnopqrstuvB"),
                holder.apiKeyHashes(), "both pushed bcrypt hashes are captured live, in order");
    }

    @Test
    void snapshotApply_reconcilesTheDoclingStatefulInstance_withTheEntitledValue() {
        DaemonCommandOutcome enabled = handler.handle(command("c-docling-on",
                "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                        + "\"webhookRelay\":false,\"doclingEnabled\":true}"));
        assertTrue(enabled.succeeded());
        verify(doclingContainer, timeout(1000)).reconcile(true);

        DaemonCommandOutcome disabled = handler.handle(command("c-docling-off",
                "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                        + "\"webhookRelay\":false,\"doclingEnabled\":false}"));
        assertTrue(disabled.succeeded());
        verify(doclingContainer, timeout(1000)).reconcile(false);
    }

    @Test
    void noPayloadReload_doesNotTouchDocling() {
        handler.handle(command("c-noop2", null));
        org.mockito.Mockito.verifyNoInteractions(doclingContainer);
    }

    @Test
    void wp3SnapshotWithoutApiKeyHashes_isBackwardCompatible_emptyHashes() {
        // A (five-field) snapshot must still parse and simply carry NO key material — the additive
        // field is unknown-field tolerant and normalises to an immutable empty list.
        String payload = "{\"logRetentionDays\":30,\"fixOpsEnabled\":false,\"monitoringEnabled\":true,"
                + "\"webhookRelay\":false,\"doclingEnabled\":false}";

        DaemonCommandOutcome outcome = handler.handle(command("c-wp3", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertEquals(30, holder.logRetentionDays(), "WP-3 fields still apply verbatim");
        assertFalse(holder.webhookRelay());
        assertTrue(holder.apiKeyHashes().isEmpty(), "no apiKeyHashes field → an empty (never null) list");
        assertFalse(holder.current().hasApiKeyHashes());
    }

    // The crossEnvInbound grant set rides the same snapshot

    @Test
    void snapshotWithCrossEnvInbound_replacesTheGrantStoreWholesale() {
        String payload = "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":false,\"doclingEnabled\":false,"
                + "\"crossEnvInbound\":["
                + "{\"callerEnvId\":\"env-a\",\"scopes\":[\"execute\"]},"
                + "{\"callerEnvId\":\"*\",\"scopes\":[\"llm\"]}]}";

        DaemonCommandOutcome outcome = handler.handle(command("c-grants", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertTrue(grantStore.check("env-a", "execute"), "exact caller grant applied live");
        assertTrue(grantStore.check("env-a", "llm"),
                "the wildcard caller covers llm for ANY caller — env-a included");
        assertTrue(grantStore.check("env-b", "llm"), "wildcard caller grant applied live");
        assertFalse(grantStore.check("env-b", "execute"), "no execute grant for env-b");

        // A later push REPLACES the set: the env-a grant is gone, only the new row remains.
        String second = "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":false,\"doclingEnabled\":false,"
                + "\"crossEnvInbound\":[{\"callerEnvId\":\"env-c\",\"scopes\":[\"execute\"]}]}";
        assertTrue(handler.handle(command("c-grants-2", second)).succeeded());
        assertFalse(grantStore.check("env-a", "execute"), "revoked by wholesale replace");
        assertFalse(grantStore.check("env-b", "llm"), "wildcard revoked by wholesale replace");
        assertTrue(grantStore.check("env-c", "execute"), "the new row is in force");
    }

    @Test
    void snapshotWithoutCrossEnvInbound_clearsTheGrantStore() {
        // Absent field → empty list (backward-compatible deserialization) → deny-all. An empty
        // array is the backend saying "nobody may call"; both must land as an empty set.
        grantStore.replace(java.util.List.of(
                new com.studioquack.sqs.entitlement.CrossEnvInboundGrant("env-a",
                        java.util.List.of("execute"))));
        assertTrue(grantStore.check("env-a", "execute"), "pre-seeded grant present");

        DaemonCommandOutcome outcome = handler.handle(command("c-no-grants",
                "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                        + "\"webhookRelay\":false,\"doclingEnabled\":false}"));

        assertTrue(outcome.succeeded());
        assertFalse(grantStore.check("env-a", "execute"),
                "a snapshot without crossEnvInbound normalises to empty → deny-all");
        assertEquals(0, grantStore.size());
    }

    @Test
    void noPayloadReload_leavesTheGrantStoreUntouched() {
        grantStore.replace(java.util.List.of(
                new com.studioquack.sqs.entitlement.CrossEnvInboundGrant("env-a",
                        java.util.List.of("execute"))));

        DaemonCommandOutcome outcome = handler.handle(command("c-noop-grants", null));

        assertTrue(outcome.succeeded());
        assertTrue(grantStore.check("env-a", "execute"),
                "a no-payload operator reload must not touch the inbound grant set");
    }

    // ---- Freshness guard: a stale snapshot must never revert a newer applied one ----------

    @Test
    void staleSnapshot_isIgnored_andTouchesNothing() throws Exception {
        DaemonCommandOutcome fresh = handler.handle(new DaemonCommand("c-new",
                ReloadConfigCommandHandler.TYPE, "2026-08-26T01:49:10Z",
                "{\"logRetentionDays\":30,\"fixOpsEnabled\":true,\"monitoringEnabled\":false,"
                        + "\"webhookRelay\":false,\"doclingEnabled\":true}"));
        assertTrue(fresh.succeeded());
        assertTrue(holder.doclingEnabled());
        verify(doclingContainer, timeout(1000)).reconcile(true);

        // The incident shape: a snapshot enqueued while the daemon was offline lands AFTER
        // the newer push (cross-transport reorder). Its older issuedAt must be ignored —
        // entitlements, the grant store and the Docling reconcile all stay untouched.
        DaemonCommandOutcome stale = handler.handle(new DaemonCommand("c-old",
                ReloadConfigCommandHandler.TYPE, "2026-08-26T01:48:00Z",
                "{\"logRetentionDays\":1,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                        + "\"webhookRelay\":false,\"doclingEnabled\":false}"));

        assertTrue(stale.succeeded(), "a superseded snapshot is not an error");
        Map<?, ?> ack = mapper.readValue(stale.output(), Map.class);
        assertEquals("stale-ignored", ack.get("reload"));
        assertTrue(holder.doclingEnabled(), "a stale snapshot must not revert fresh state");
        assertEquals(30, holder.logRetentionDays());
        org.mockito.Mockito.verifyNoMoreInteractions(doclingContainer);
    }

    @Test
    void equalStamp_stillApplies() {
        // Duplicate delivery of the SAME push on both transports carries an equal stamp —
        // not strictly older — and must still apply (idempotently).
        String payload = "{\"logRetentionDays\":7,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":false,\"doclingEnabled\":false}";
        assertTrue(handler.handle(new DaemonCommand("c-a", ReloadConfigCommandHandler.TYPE,
                "2026-08-26T01:49:10Z", payload)).succeeded());
        assertTrue(handler.handle(new DaemonCommand("c-b", ReloadConfigCommandHandler.TYPE,
                "2026-08-26T01:49:10Z", payload)).succeeded());
        assertEquals(7, holder.logRetentionDays());
    }

    @Test
    void blankOrGarbageIssuedAt_failsOpen() {
        // A missing/unparseable stamp must never brick entitlement delivery.
        String payload = "{\"logRetentionDays\":9,\"fixOpsEnabled\":false,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":false,\"doclingEnabled\":false}";
        assertTrue(handler.handle(new DaemonCommand("c-nostamp",
                ReloadConfigCommandHandler.TYPE, null, payload)).succeeded());
        assertEquals(9, holder.logRetentionDays());
        assertTrue(handler.handle(new DaemonCommand("c-garbage",
                ReloadConfigCommandHandler.TYPE, "not-an-instant", payload)).succeeded());
    }

    // ---- Wire-compat: Fix/Ops retired; extra key ignored, missing key still applies ----------

    @Test
    void oldBeOrOldDiskPayload_withFixOpsEnabledKey_isIgnoredAndAppliesRemainingFields() {
        // Production-incident guard: an older caller / old sealed snapshot still emit
        // fixOpsEnabled. ignoreUnknown swallows it; remaining current fields must apply.
        String payload = "{\"logRetentionDays\":30,\"fixOpsEnabled\":true,"
                + "\"monitoringEnabled\":false,\"webhookRelay\":true,\"doclingEnabled\":false,"
                + "\"webhookMessageEventsEnabled\":false,\"logPropagation\":false}";

        DaemonCommandOutcome outcome = handler.handle(command("c-old-fixops", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertEquals(30, holder.logRetentionDays());
        assertTrue(holder.current().webhookRelay());
        assertFalse(holder.current().monitoringEnabled());
    }

    @Test
    void newBePayload_withoutFixOpsEnabled_appliesRemainingFields() {
        String payload = "{\"logRetentionDays\":30,"
                + "\"monitoringEnabled\":false,\"webhookRelay\":true,\"doclingEnabled\":false,"
                + "\"webhookMessageEventsEnabled\":false,\"logPropagation\":false}";

        DaemonCommandOutcome outcome = handler.handle(command("c-new-be", payload));

        assertTrue(outcome.succeeded(), () -> "apply outcome error: " + outcome.error());
        assertEquals(30, holder.logRetentionDays());
        assertTrue(holder.current().webhookRelay());
        assertFalse(holder.current().monitoringEnabled());
    }
}
