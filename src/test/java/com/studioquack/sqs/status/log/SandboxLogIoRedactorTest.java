package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DEFECT 2 — a COMPLETED sandbox-log row must ship parseable input/output JSON (hydrated
 * object or spill marker), never Java null, even when Jackson {@code convertValue} cannot
 * deep-copy a large ContentBlock tree.
 */
class SandboxLogIoRedactorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Bigger than {@link SandboxLogIoRedactor#MAX_INLINE_DATA_CHARS} so the redactor strips it. */
    private static final int LARGE_DATA_CHARS = 19_000_000;
    private static final String FILE_REF = "sq-file:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    /**
     * Jackson {@code convertValue} does not apply {@code StreamReadConstraints} (TokenBuffer),
     * so oversized {@code data} alone will not throw. A nested getter that throws is the
     * catchable failure the production {@code catch (Exception)} turns into {@code null}.
     */
    static final class Unconvertible {
        public String getData() {
            throw new IllegalStateException("jackson-cannot-convert");
        }
    }

    @Test
    void largeFileBlockInput_convertValueFailure_mappedRowInputNeverNull() throws Exception {
        List<InOutValueDto> input = fileBlockPort("messages", LARGE_DATA_CHARS, "sheet.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", true);
        List<InOutValueDto> output = fileBlockPort("result", LARGE_DATA_CHARS, "preview.png",
                "image/png", false);

        assertThatThrownBy(() -> MAPPER.convertValue(input, Object.class))
                .isInstanceOf(Exception.class);

        Object redactedInput = SandboxLogIoRedactor.redactInput(MAPPER, input);
        Object redactedOutput = SandboxLogIoRedactor.redactOutput(MAPPER, output);

        Map<String, byte[]> spilled = new LinkedHashMap<>();
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.store(eq(ClientFileStore.SCOPE_RUN), eq("run-child"), any(),
                eq("application/json"), any(byte[].class))).thenAnswer(inv -> {
            String name = inv.getArgument(2);
            String ref = FILE_REF + "-" + name;
            spilled.put(ref, inv.getArgument(4));
            return ref;
        });
        when(files.resolve(any())).thenAnswer(inv -> {
            byte[] bytes = spilled.get(inv.getArgument(0));
            return bytes == null ? Optional.empty() : Optional.of(
                    new ClientFileStore.StoredFile("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                            "io.json", "application/json", bytes.length, bytes));
        });

        SandboxLogRepository repository = mock(SandboxLogRepository.class);
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        SandboxLogStore store = new SandboxLogStore(repository, MAPPER);
        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-child", "node-child", "Child", "COMPLETED", 0, 12L, "python", "ctr-1",
                null, null, null, redactedInput, redactedOutput, null, null, "NONE", null));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        SandboxLogEntity saved = captor.getValue();

        SandboxLogRowMapper rowMapper = new SandboxLogRowMapper(MAPPER,
                new SandboxLogFileHydrator(MAPPER, providerOf(files)));
        SandboxLogWireRow row = rowMapper.toWireRow(saved, SandboxLogRowMapper.FileMode.HYDRATE);

        assertThat(row.input()).as("COMPLETED input must be marker or hydrated JSON, never null")
                .isNotNull();
        assertThat(row.output()).as("COMPLETED output path must stay non-null")
                .isNotNull();
        String inputJson = MAPPER.writeValueAsString(row.input());
        String outputJson = MAPPER.writeValueAsString(row.output());
        assertThat(inputJson).isNotEqualTo("null");
        assertThat(outputJson).isNotEqualTo("null");
        assertThat(inputJson).contains("sheet.xlsx");
        assertThat(outputJson).contains("preview.png");
        assertThat(saved.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    void largeFileBlockOutput_defaultMapper_spillsAndHydrates() throws Exception {
        List<InOutValueDto> output = fileBlockPort("result", LARGE_DATA_CHARS, "preview.png",
                "image/png", false);
        Object redactedOutput = SandboxLogIoRedactor.redactOutput(MAPPER, output);
        assertThat(redactedOutput).isNotNull();

        String fullJson = MAPPER.writeValueAsString(redactedOutput);
        // The redactor strips the oversized inline `data` string, so the row stays well under
        // the inline cap — the heavy bytes never reach the log feed.
        assertThat(fullJson.length()).isLessThan(SandboxLogIoRedactor.MAX_INLINE_DATA_CHARS);
        byte[] fullBytes = fullJson.getBytes(StandardCharsets.UTF_8);

        ClientFileStore files = mock(ClientFileStore.class);
        when(files.store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("output.json"),
                eq("application/json"), any(byte[].class))).thenReturn(FILE_REF);
        when(files.resolve(FILE_REF)).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                        "output.json", "application/json", fullBytes.length, fullBytes)));

        SandboxLogRepository repository = mock(SandboxLogRepository.class);
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        SandboxLogStore store = new SandboxLogStore(repository, MAPPER);
        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-1", "node-1", null, "COMPLETED", 0, 1L, "python", null,
                null, null, null, null, redactedOutput, null, null, "NONE", null));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        // The oversized inline `data` string is stripped by the redactor, so the stored row
        // carries the omission marker in its place — never the heavy bytes.
        assertThat(captor.getValue().getOutputJson()).contains("<omitted");

        SandboxLogRowMapper rowMapper = new SandboxLogRowMapper(MAPPER,
                new SandboxLogFileHydrator(MAPPER, providerOf(files)));
        SandboxLogWireRow row = rowMapper.toWireRow(captor.getValue(), SandboxLogRowMapper.FileMode.HYDRATE);
        assertThat(row.output()).isNotNull();
        // The file's identity survives even though its bytes are omitted.
        assertThat(MAPPER.writeValueAsString(row.output())).contains("preview.png");
    }

    @Test
    void nullPayload_staysNull() {
        assertThat(SandboxLogIoRedactor.redactOutput(MAPPER, null)).isNull();
    }

    @Test
    void outputUnfilteredValue_isWhatDuckTalesSeesAsValue() {
        InOutValueDto dto = new InOutValueDto();
        dto.setPortValue("out");
        dto.setValue(Map.of());
        dto.setUnfilteredValue(Map.of("result", Map.of(
                "kind", "ok",
                "files", List.of(Map.of("name", "generated.png", "kind", "image")))));

        Object redacted = SandboxLogIoRedactor.redactOutput(MAPPER, List.of(dto));
        assertThat(redacted).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) ((List<?>) redacted).get(0);
        assertThat(row.get("portValue")).isEqualTo("out");
        assertThat(row).doesNotContainKey("unfilteredValue");
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) row.get("value");
        assertThat(value).containsKey("result");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) value.get("result");
        assertThat(result.get("files")).asList().hasSize(1);
    }

    /**
     * Producer fan-out stamps unfilteredValue with the source-shaped envelope. After inbound
     * JOLT, {@code value} is the mapped consumer payload. The Input tab must log that, not
     * the producer's leftover keys.
     */
    @Test
    void inputLane_keepsPostJoltValue_ignoresProducerUnfiltered() {
        InOutValueDto dto = new InOutValueDto();
        dto.setPortValue("out");
        dto.setTargetPortValue("in");
        dto.setNodeId("producer");
        dto.setTargetNodeId("consumer");
        dto.setValue(Map.of("x", "from-a"));
        dto.setUnfilteredValue(Map.of("a", "from-a", "b", "from-b"));

        Object redacted = SandboxLogIoRedactor.redactInput(MAPPER, List.of(dto));
        assertThat(redacted).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) ((List<?>) redacted).get(0);
        assertThat(row).doesNotContainKey("unfilteredValue");
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) row.get("value");
        assertThat(value).isEqualTo(Map.of("x", "from-a"));
        assertThat(value).doesNotContainKeys("a", "b");
    }

    private static List<InOutValueDto> fileBlockPort(String portValue, int dataChars,
                                                     String name, String mimeType,
                                                     boolean includeUnconvertible) {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "file");
        block.put("mime_type", mimeType);
        block.put("name", name);
        block.put("source", "base64");
        // Base64-ish alphabet, not a real file — size is what trips the 256 KiB spill.
        block.put("data", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef".repeat(Math.max(1, dataChars / 32))
                .substring(0, dataChars));

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("role", "user");
        message.put("content", List.of(block));

        List<Object> value = new ArrayList<>();
        value.add(message);
        if (includeUnconvertible) {
            value.add(new Unconvertible());
        }

        InOutValueDto dto = new InOutValueDto();
        dto.setPortValue(portValue);
        dto.setValue(value);
        List<InOutValueDto> list = new ArrayList<>();
        list.add(dto);
        return list;
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
