package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.files.ClientFileStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The two non-live log lanes differ in ONE thing: whether {@code sq-file:} handles become inline
 * base64. HYDRATE is the terminal BATCH flush (persisted by the backend, spilled to GridFS, one
 * shipment).
 */
class SandboxLogRowMapperFileModeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String REF = "sq-file:11111111-2222-3333-4444-555555555555";
    private static final byte[] BYTES = "png-bytes".getBytes(StandardCharsets.UTF_8);

    private static SandboxLogEntity rowWithFileHandle() throws Exception {
        Map<String, Object> block = Map.of(
                "kind", "file", "name", "defect.png", "mime_type", "image/png", "fileId", REF);
        SandboxLogEntity e = new SandboxLogEntity();
        e.setNodeId("n1");
        e.setStatus("COMPLETED");
        e.setInputJson(MAPPER.writeValueAsString(List.of(Map.of("portValue", "in", "value", List.of(block)))));
        e.setOutputJson(MAPPER.writeValueAsString(List.of(Map.of("portValue", "out", "value", block))));
        e.setAppliedInputsJson(MAPPER.writeValueAsString(Map.of("in", List.of(block))));
        return e;
    }

    private static SandboxLogRowMapper mapperOver(ClientFileStore files) {
        return new SandboxLogRowMapper(MAPPER, new SandboxLogFileHydrator(MAPPER, providerOf(files)));
    }

    @Test
    void refOnly_keepsTheHandleAndNeverReadsTheFileStore() throws Exception {
        ClientFileStore files = mock(ClientFileStore.class);

        SandboxLogWireRow row = mapperOver(files).toWireRow(rowWithFileHandle(),
                SandboxLogRowMapper.FileMode.REF_ONLY);

        String input = MAPPER.writeValueAsString(row.input());
        String output = MAPPER.writeValueAsString(row.output());
        String applied = MAPPER.writeValueAsString(row.appliedInputs());
        for (String json : List.of(input, output, applied)) {
            assertThat(json).contains(REF).contains("defect.png").contains("image/png");
            assertThat(json).doesNotContain("\"data\"");
        }
        verify(files, never()).resolve(any());
    }

    @Test
    void hydrate_inlinesBytesOnceForTheTerminalFlush() throws Exception {
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.resolve(REF)).thenReturn(Optional.of(new ClientFileStore.StoredFile(
                "11111111-2222-3333-4444-555555555555", "defect.png", "image/png", BYTES.length, BYTES)));

        SandboxLogWireRow row = mapperOver(files).toWireRow(rowWithFileHandle(),
                SandboxLogRowMapper.FileMode.HYDRATE);

        String output = MAPPER.writeValueAsString(row.output());
        assertThat(output).contains("\"data\"").contains("\"source\":\"base64\"");
    }

    private static ObjectProvider<ClientFileStore> providerOf(ClientFileStore store) {
        return new ObjectProvider<>() {
            @Override
            public ClientFileStore getObject() {
                return store;
            }

            @Override
            public ClientFileStore getIfAvailable() {
                return store;
            }
        };
    }
}
