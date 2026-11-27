package com.studioquack.sqs.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeployedWebhookBindingStoreTest {

    private Map<String, DeployedWebhookBindingEntity> db;
    private DeployedWebhookBindingRepository repo;
    private DeployedWebhookBindingStore store;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        db = new LinkedHashMap<>();
        mapper = new ObjectMapper();
        repo = mock(DeployedWebhookBindingRepository.class);

        when(repo.findByWebhookKeyRef(anyString())).thenAnswer(inv -> {
            String key = inv.getArgument(0);
            return db.values().stream().filter(b -> key.equals(b.getWebhookKeyRef())).toList();
        });
        when(repo.findByWorkflowIdAndNamespaceKey(anyString(), anyString())).thenAnswer(inv -> {
            String wf = inv.getArgument(0);
            String ns = inv.getArgument(1);
            return db.values().stream()
                    .filter(b -> wf.equals(b.getWorkflowId()) && ns.equals(b.getNamespaceKey()))
                    .toList();
        });
        when(repo.findByWorkflowId(anyString())).thenAnswer(inv -> {
            String wf = inv.getArgument(0);
            return db.values().stream().filter(b -> wf.equals(b.getWorkflowId())).toList();
        });
        when(repo.deleteByWorkflowIdAndNamespaceKey(anyString(), anyString())).thenAnswer(inv -> {
            String wf = inv.getArgument(0);
            String ns = inv.getArgument(1);
            List<String> keys = db.values().stream()
                    .filter(b -> wf.equals(b.getWorkflowId()) && ns.equals(b.getNamespaceKey()))
                    .map(DeployedWebhookBindingEntity::getBindingId).toList();
            keys.forEach(db::remove);
            return (long) keys.size();
        });
        when(repo.deleteByWorkflowId(anyString())).thenAnswer(inv -> {
            String wf = inv.getArgument(0);
            List<String> keys = db.values().stream()
                    .filter(b -> wf.equals(b.getWorkflowId()))
                    .map(DeployedWebhookBindingEntity::getBindingId).toList();
            keys.forEach(db::remove);
            return (long) keys.size();
        });
        doAnswer(inv -> {
            Iterable<DeployedWebhookBindingEntity> items = inv.getArgument(0);
            items.forEach(e -> db.put(e.getBindingId(), e));
            return items;
        }).when(repo).saveAll(any());

        store = new DeployedWebhookBindingStore(repo, mapper);
    }

    @Test
    void replaceForWorkflow_indexesNodeAndBoundaryBindings_andReplacesAtomically() {
        String keyA = "whk_aaaaaaaaaaaaaaaaaaaaaaaa";
        String keyB = "whk_bbbbbbbbbbbbbbbbbbbbbbbb";

        NodeDto listen = roleNode("n1", "tpl-1", "listen:Listen:listen", "listen", keyA);
        NodeDto normalIgnored = roleNode("n_norm", "tpl-1", "send:Send", "send", keyA);
        NodeDto hostWithBoundary = boundaryHostNode("n_host", "b1", "tpl-b", keyB);

        int count = store.replaceForWorkflow("wf1", "", "def:1", List.of(listen, normalIgnored, hostWithBoundary));
        assertEquals(2, count, "normal role node must not be indexed");

        List<DeployedWebhookBindingEntity> forA = store.findByWebhookKeyRef(keyA);
        assertEquals(1, forA.size());
        assertEquals(DeployedWebhookBindingStore.KIND_MESSAGE_START, forA.get(0).getBindingKind());
        assertEquals(WebhookMessageNames.forNode("wf1", "n1"), forA.get(0).getMessageName());

        List<DeployedWebhookBindingEntity> forB = store.findByWebhookKeyRef(keyB);
        assertEquals(1, forB.size());
        assertEquals(DeployedWebhookBindingStore.KIND_BOUNDARY_MESSAGE, forB.get(0).getBindingKind());
        assertEquals("b1", forB.get(0).getBoundaryId());
        assertNotNull(forB.get(0).getTargetNodeJson());
        assertTrue(forB.get(0).getTargetNodeJson().contains(keyB));

        // Atomic replace removes old bindings and invalidates cache.
        NodeDto sw = roleNode("n2", "tpl-2", "sw:SendWait:send_and_wait", "sw", keyB);
        store.replaceForWorkflow("wf1", "", "def:2", List.of(sw));
        assertTrue(store.findByWebhookKeyRef(keyA).isEmpty(), "cache and DB must reflect removal of keyA");
        List<DeployedWebhookBindingEntity> updatedB = store.findByWebhookKeyRef(keyB);
        assertEquals(1, updatedB.size());
        assertEquals(DeployedWebhookBindingStore.KIND_SEND_AND_WAIT, updatedB.get(0).getBindingKind());
    }

    @Test
    void enforcesMaxTwentyMessageStartBindingsPerWebhookKey() {
        String sharedKey = "whk_cccccccccccccccccccccccc";
        for (int i = 1; i <= DeployedWebhookBindingStore.MAX_MESSAGE_START_BINDINGS_PER_KEY; i++) {
            store.replaceForWorkflow("wf_" + i, "", "def:" + i,
                    List.of(roleNode("n1", "tpl-1", "listen:Listen:listen", "listen", sharedKey)));
        }
        assertEquals(20, store.findByWebhookKeyRef(sharedKey).size());

        // Redeploying an existing workflow replaces in place and stays at 20.
        store.replaceForWorkflow("wf_1", "", "def:1b",
                List.of(roleNode("n1", "tpl-1", "listen:Listen:listen", "listen", sharedKey)));
        assertEquals(20, store.findByWebhookKeyRef(sharedKey).size());

        // 21st distinct workflow MESSAGE_START on the same key must fail without mutating state.
        assertThrows(IllegalArgumentException.class, () ->
                store.replaceForWorkflow("wf_21", "", "def:21",
                        List.of(roleNode("n1", "tpl-1", "listen:Listen:listen", "listen", sharedKey))));
        assertEquals(20, store.findByWebhookKeyRef(sharedKey).size());

        // Undeploying one frees a slot.
        store.deleteForWorkflow("wf_20");
        assertEquals(19, store.findByWebhookKeyRef(sharedKey).size());
        store.replaceForWorkflow("wf_21", "", "def:21",
                List.of(roleNode("n1", "tpl-1", "listen:Listen:listen", "listen", sharedKey)));
        assertEquals(20, store.findByWebhookKeyRef(sharedKey).size());
    }

    private static NodeDto roleNode(String nodeId, String tplId, String option, String value, String keyRef) {
        ParameterDto sel = new ParameterDto();
        sel.setShortCode("operation");
        sel.setType("select");
        sel.setOptions(List.of(option));
        sel.setValue(value);

        ParameterDto wh = new ParameterDto();
        wh.setShortCode("webhook_key");
        wh.setType("webhook_key");
        wh.setValue(WebhookKeyRefHelper.placeholder(keyRef));

        TemplateDto tpl = new TemplateDto();
        tpl.setId(tplId);
        tpl.setConfigurationParameters(new ArrayList<>(List.of(sel, wh)));

        NodeDto n = new NodeDto();
        n.setId(nodeId);
        n.setData(tpl);
        return n;
    }

    private static NodeDto boundaryHostNode(String hostId, String boundaryId, String resolvedTplId, String keyRef) {
        TemplateDto resolved = new TemplateDto();
        resolved.setId(resolvedTplId);
        ParameterDto op = new ParameterDto();
        op.setShortCode("operation");
        op.setType("select");
        op.setOptions(List.of("listen:Listen:listen"));
        op.setValue("send");
        resolved.setConfigurationParameters(List.of(op));

        Map<String, Object> binding = new LinkedHashMap<>();
        binding.put("templateId", resolvedTplId);
        binding.put("listenParamShortCode", "operation");
        binding.put("listenParamValue", "listen");
        binding.put("resolvedTemplate", resolved);
        binding.put("configurationParameters", List.of(
                Map.of("shortCode", "webhook_key", "value", WebhookKeyRefHelper.placeholder(keyRef))));

        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setId(boundaryId);
        cfg.setType(BoundaryEventConfig.BoundaryEventType.MESSAGE);
        cfg.putConfig(BoundaryEventConfig.CONFIG_MESSAGE_SOURCE, BoundaryEventConfig.MESSAGE_SOURCE_INGRESS_NODE);
        cfg.putConfig(BoundaryEventConfig.CONFIG_INGRESS_NODE_BINDING, binding);

        TemplateDto hostTpl = new TemplateDto();
        hostTpl.setId("tpl-host");
        hostTpl.setBoundaryEventConfigs(List.of(cfg));

        NodeDto host = new NodeDto();
        host.setId(hostId);
        host.setData(hostTpl);
        return host;
    }
}
