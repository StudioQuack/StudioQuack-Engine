package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.files.ClientFileStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * I/O-spill markers must hydrate back to the original object; a failed resolve must leave
 * the marker (valid JSON) rather than collapsing to null.
 */
class SandboxLogFileHydratorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String REF = "sq-file:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    @Test
    void spillMarker_isReplacedWithOriginalObject() throws Exception {
        Map<String, Object> original = Map.of("k", "v", "n", 2);
        byte[] bytes = MAPPER.writeValueAsString(original).getBytes(StandardCharsets.UTF_8);
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.resolve(REF)).thenReturn(Optional.of(
                new ClientFileStore.StoredFile("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                        "input.json", "application/json", bytes.length, bytes)));
        SandboxLogFileHydrator hydrator = new SandboxLogFileHydrator(MAPPER, providerOf(files));

        Object restored = hydrator.hydrate(spillMarker(REF, bytes.length));

        assertThat(restored).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) restored).get("k")).isEqualTo("v");
        assertThat(((Number) ((Map<?, ?>) restored).get("n")).intValue()).isEqualTo(2);
    }

    @Test
    void spillMarker_failedResolve_leavesMarkerNonNull() {
        ClientFileStore files = mock(ClientFileStore.class);
        when(files.resolve(REF)).thenReturn(Optional.empty());
        SandboxLogFileHydrator hydrator = new SandboxLogFileHydrator(MAPPER, providerOf(files));
        Map<String, Object> marker = spillMarker(REF, 42);

        Object result = hydrator.hydrate(marker);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) result).get("sqFileRef")).isEqualTo(REF);
        assertThat(((Map<?, ?>) result).get("truncated")).isEqualTo(true);
    }

    private static Map<String, Object> spillMarker(String sqFileRef, int originalBytes) {
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("sqFileRef", sqFileRef);
        marker.put("truncated", true);
        marker.put("originalBytes", originalBytes);
        return marker;
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
