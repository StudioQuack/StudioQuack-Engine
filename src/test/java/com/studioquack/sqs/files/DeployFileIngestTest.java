package com.studioquack.sqs.files;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deploy-time hand-off: the backend's embedded base64 becomes a sealed, daemon-held file. Leaving
 * the base64 on the parameter would put the file in cleartext inside the artifact store and
 * re-serialise it into every execution context that touches the node, which is exactly what the
 * client-side file store exists to prevent.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-deployfile-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.files.max-bytes=4096"
})
@ActiveProfiles("base")
class DeployFileIngestTest {

    @Autowired
    private DeployFileIngest ingest;
    @Autowired
    private ClientFileStore store;

    private static Map<String, Object> descriptor(String name, String mime, byte[] content, String omitted) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("fileId", "gridfs-1");
        d.put("name", name);
        d.put("mimeType", mime);
        d.put("size", content == null ? 0 : content.length);
        if (content != null) {
            d.put("contentBase64", Base64.getEncoder().encodeToString(content));
        }
        if (omitted != null) {
            d.put("omitted", omitted);
        }
        return Map.of(DeployFileIngest.FILE_REF_KEY, d);
    }

    private static NodeDto node(String id, ParameterDto... parameters) {
        TemplateDto template = new TemplateDto();
        template.setConfigurationParameters(new ArrayList<>(List.of(parameters)));
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(template);
        return n;
    }

    private static ParameterDto param(String id, String type, Object value) {
        ParameterDto p = new ParameterDto();
        p.setId(id);
        p.setType(type);
        p.setValue(value);
        return p;
    }

    @Test
    @DisplayName("embedded content becomes a sealed handle and the base64 leaves the graph")
    void embeddedContent_becomesHandle() {
        byte[] content = "the-document".getBytes(StandardCharsets.UTF_8);
        NodeDto n = node("n1", param("p1", "file",
                descriptor("contract.pdf", "application/pdf", content, null)));

        assertEquals(1, ingest.ingest("wf-1", List.of(n)));

        Object value = n.getData().getConfigurationParameters().get(0).getValue();
        assertInstanceOf(String.class, value, "the parameter now holds a handle, not a descriptor");
        assertTrue(ClientFileStore.isRef(value));
        assertArrayEquals(content, store.resolve(value).orElseThrow().content());
        assertEquals("contract.pdf", store.resolve(value).orElseThrow().fileName());
    }

    @Test
    @DisplayName("redeploying reclaims the previous deployment's files instead of accumulating")
    void redeploy_supersedesPreviousFiles() {
        NodeDto first = node("n1", param("p1", "file",
                descriptor("v1.pdf", "application/pdf", "one".getBytes(StandardCharsets.UTF_8), null)));
        ingest.ingest("wf-super", List.of(first));
        Object firstRef = first.getData().getConfigurationParameters().get(0).getValue();

        NodeDto second = node("n1", param("p1", "file",
                descriptor("v2.pdf", "application/pdf", "two".getBytes(StandardCharsets.UTF_8), null)));
        ingest.ingest("wf-super", List.of(second));
        Object secondRef = second.getData().getConfigurationParameters().get(0).getValue();

        assertTrue(store.resolve(firstRef).isEmpty(), "the superseded file is reclaimed");
        assertTrue(store.resolve(secondRef).isPresent(), "the new file is available");
        assertEquals(1, store.listForScope(ClientFileStore.SCOPE_DEPLOY, "wf-super").size());
    }

    @Test
    @DisplayName("a descriptor the backend sent without content is left in place for a precise failure")
    void omittedContent_isLeftAsDescriptor() {
        // The backend omits content above its inline limit. Silently dropping the parameter would make
        // the node fail with "missing value" instead of "the file was too large to deploy".
        NodeDto n = node("n1", param("p1", "file",
                descriptor("huge.pdf", "application/pdf", null, "exceeds-deploy-inline-limit")));

        assertEquals(0, ingest.ingest("wf-2", List.of(n)));

        assertInstanceOf(Map.class, n.getData().getConfigurationParameters().get(0).getValue());
    }

    @Test
    @DisplayName("a file above the client limit does not fail the deploy")
    void oversizedFile_doesNotFailTheDeploy() {
        // A graph that cannot carry one file is more useful than no deployment at all.
        NodeDto n = node("n1", param("p1", "file",
                descriptor("big.bin", null, new byte[5000], null)));

        assertEquals(0, ingest.ingest("wf-3", List.of(n)));
    }

    @Test
    @DisplayName("non-file parameters and plain values are untouched")
    void otherParameters_areIgnored() {
        NodeDto n = node("n1",
                param("p1", "string", "hello"),
                param("p2", "file", "0_legacy.pdf"),
                param("p3", "file", null));

        assertEquals(0, ingest.ingest("wf-4", List.of(n)));

        assertEquals("hello", n.getData().getConfigurationParameters().get(0).getValue());
        assertEquals("0_legacy.pdf", n.getData().getConfigurationParameters().get(1).getValue(),
                "a legacy placeholder is left alone rather than guessed at");
    }

    @Test
    @DisplayName("several nodes and several file parameters are all converted")
    void multipleFiles_areAllConverted() {
        NodeDto a = node("n1",
                param("p1", "file", descriptor("a.txt", null, "A".getBytes(StandardCharsets.UTF_8), null)),
                param("p2", "file", descriptor("b.txt", null, "B".getBytes(StandardCharsets.UTF_8), null)));
        NodeDto b = node("n2",
                param("p3", "file", descriptor("c.txt", null, "C".getBytes(StandardCharsets.UTF_8), null)));

        assertEquals(3, ingest.ingest("wf-5", List.of(a, b)));
        assertEquals(3, store.listForScope(ClientFileStore.SCOPE_DEPLOY, "wf-5").size());
    }

    @Test
    @DisplayName("empty and null inputs are no-ops")
    void nullSafety() {
        assertEquals(0, ingest.ingest("wf-6", null));
        assertEquals(0, ingest.ingest("wf-6", List.of()));
        assertEquals(0, ingest.ingest(null, List.of(node("n1"))));
    }
}
