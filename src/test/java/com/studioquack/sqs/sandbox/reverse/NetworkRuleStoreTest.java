package com.studioquack.sqs.sandbox.reverse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The projected rule set, both views: wholesale replacement (a deleted rule must not linger),
 * per-port source lookup, wire parsing, the restart-surviving disk round-trip, and the change
 * listener that reprograms live containers' egress.
 */
class NetworkRuleStoreTest {

    @TempDir
    Path tempDir;

    private NetworkRuleStore store() {
        return new NetworkRuleStore(
                tempDir.resolve("network-rules.json").toString(), new ObjectMapper());
    }

    @Test
    void allowedSourcesAreScopedPerPort() {
        NetworkRuleStore store = store();
        store.replaceAll(List.of(
                new NetworkRuleStore.Rule("engine-2", 2222),
                new NetworkRuleStore.Rule("engine-3", 2222),
                new NetworkRuleStore.Rule("engine-2", 5432)), List.of());

        assertEquals(Set.of("engine-2", "engine-3"), store.allowedSourceEngineIds(2222));
        assertEquals(Set.of("engine-2"), store.allowedSourceEngineIds(5432));
        assertTrue(store.allowedSourceEngineIds(8080).isEmpty(), "no rule → deny all");
    }

    @Test
    void replaceAllIsWholesale_neverADelta() {
        NetworkRuleStore store = store();
        store.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", 2222)),
                List.of(new NetworkRuleStore.OutboundRule("engine-4", 2222)));
        store.replaceAll(List.of(new NetworkRuleStore.Rule("engine-3", 5432)), List.of());

        assertTrue(store.allowedSourceEngineIds(2222).isEmpty(),
                "a rule absent from the replacement set must be gone");
        assertEquals(Set.of("engine-3"), store.allowedSourceEngineIds(5432));
        assertTrue(store.outboundSnapshot().isEmpty(),
                "the outbound view is wholesale too — a deleted grant must not linger");
    }

    @Test
    void outboundViewIsWhatMyContainersMayDial() {
        NetworkRuleStore store = store();
        store.replaceAll(List.of(),
                List.of(new NetworkRuleStore.OutboundRule("engine-4", 2222),
                        new NetworkRuleStore.OutboundRule("engine-5", 5432),
                        new NetworkRuleStore.OutboundRule("", 1),
                        new NetworkRuleStore.OutboundRule("engine-6", 70000)));

        assertEquals(Set.of(new NetworkRuleStore.OutboundRule("engine-4", 2222),
                        new NetworkRuleStore.OutboundRule("engine-5", 5432)),
                store.outboundSnapshot());
    }

    @Test
    void malformedRowsAreDroppedNotStored() {
        NetworkRuleStore store = store();
        store.replaceAll(List.of(
                new NetworkRuleStore.Rule("", 2222),
                new NetworkRuleStore.Rule("engine-2", 0),
                new NetworkRuleStore.Rule("engine-2", 70000),
                new NetworkRuleStore.Rule("engine-2", 2222)), List.of());

        assertEquals(1, store.snapshot().size());
        assertEquals(Set.of("engine-2"), store.allowedSourceEngineIds(2222));
    }

    @Test
    void persistedSetSurvivesARestart() {
        NetworkRuleStore first = store();
        first.replaceAll(List.of(new NetworkRuleStore.Rule("engine-2", 2222)),
                List.of(new NetworkRuleStore.OutboundRule("engine-4", 9090)));

        NetworkRuleStore restarted = store();
        assertEquals(Set.of("engine-2"), restarted.allowedSourceEngineIds(2222),
                "a daemon restart between pushes must not drop the projection");
        assertEquals(Set.of(new NetworkRuleStore.OutboundRule("engine-4", 9090)),
                restarted.outboundSnapshot(),
                "the outbound view survives the restart too");
    }

    @Test
    void replacementNotifiesTheChangeListener() {
        NetworkRuleStore store = store();
        java.util.concurrent.atomic.AtomicInteger fired = new java.util.concurrent.atomic.AtomicInteger();
        store.setChangeListener(fired::incrementAndGet);

        store.replaceAll(List.of(), List.of());
        assertEquals(1, fired.get(),
                "every wholesale replacement must notify — the egress manager reprograms from it");
    }

    @Test
    void parseReadsTheWireShape() throws Exception {
        List<NetworkRuleStore.Rule> parsed = NetworkRuleStore.parse(new ObjectMapper().readTree(
                "{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222},"
                        + "{\"sourceEnvId\":\"\",\"port\":1},{\"port\":9},"
                        + "{\"sourceEnvId\":\"engine-3\",\"port\":0}]}"));

        assertEquals(1, parsed.size());
        assertEquals(new NetworkRuleStore.Rule("engine-2", 2222), parsed.get(0));
    }

    @Test
    void parseOutboundReadsTheMirrorShape() throws Exception {
        List<NetworkRuleStore.OutboundRule> parsed = NetworkRuleStore.parseOutbound(
                new ObjectMapper().readTree(
                        "{\"outbound\":[{\"targetEnvId\":\"engine-4\",\"port\":2222},"
                                + "{\"targetEnvId\":\"\",\"port\":1},{\"port\":9},"
                                + "{\"targetEnvId\":\"engine-5\",\"port\":0}]}"));

        assertEquals(1, parsed.size());
        assertEquals(new NetworkRuleStore.OutboundRule("engine-4", 2222), parsed.get(0));
        assertTrue(NetworkRuleStore.parseOutbound(new ObjectMapper().readTree("{}")).isEmpty(),
                "a payload without the outbound array means an empty outbound view");
        assertTrue(NetworkRuleStore.parseOutbound(null).isEmpty());
    }

    @Test
    void parseToleratesMissingRulesArray() throws Exception {
        assertTrue(NetworkRuleStore.parse(new ObjectMapper().readTree("{}")).isEmpty());
        assertTrue(NetworkRuleStore.parse(null).isEmpty());
    }
}
