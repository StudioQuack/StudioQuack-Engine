package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Deployed workflows and stored templates still carry the removed per-node {@code network}
 * posture object; the daemon must keep reading them even through a strict mapper.
 */
class ExecutionRequirementsDtoJsonTest {

    private static final ObjectMapper STRICT = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static final String LEGACY_NETWORK = "\"network\":{\"noInternet\":true,"
            + "\"noLocal\":false,\"noReverse\":false,\"reversePort\":5432}";

    @Test
    void legacyNetworkObjectIsIgnored() throws Exception {
        String legacy = "{\"cpu\":2,\"ram\":1024,\"disc\":4096,\"ociImageRef\":\"myenv/pg:1\","
                + LEGACY_NETWORK + "}";

        ExecutionRequirementsDto req = STRICT.readValue(legacy, ExecutionRequirementsDto.class);

        assertEquals(2, req.getCpu());
        assertEquals(1024, req.getRam());
        assertEquals(4096, req.getDisc());
        assertEquals("myenv/pg:1", req.getOciImageRef());
    }

    @Test
    void deployedTemplateWithLegacyNetworkStillDeserializes() throws Exception {
        String template = "{\"id\":\"tpl-1\",\"executionRequirementsDto\":{\"cpu\":1,"
                + LEGACY_NETWORK + "}}";

        TemplateDto dto = STRICT.readValue(template, TemplateDto.class);

        assertEquals("tpl-1", dto.getId());
        assertEquals(1, dto.getExecutionRequirementsDto().getCpu());
    }
}
