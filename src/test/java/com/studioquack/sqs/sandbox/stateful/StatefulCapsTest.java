package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire decode for stateful {@code caps:{cpu,ramMb,diskMb,gpu?}}. Argv application is covered by
 * {@link StatefulRunArgsTest} (cpu/ram/disk/gpu → {@code --cpus}/{@code --memory}/
 * {@code --storage-opt}/{@code --gpus}) — this class pins {@link StatefulCaps#fromJson} only.
 */
class StatefulCapsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void fromJson_decodes_cpu_ram_disk_and_gpu() throws Exception {
        StatefulCaps caps = StatefulCaps.fromJson(mapper.readTree(
                "{\"cpu\":2,\"ramMb\":1024,\"diskMb\":4096,\"gpu\":1}"));

        assertEquals(2, caps.cpu());
        assertEquals(1024, caps.ramMb());
        assertEquals(4096, caps.diskMb());
        assertEquals(1, caps.gpu());
        assertTrue(caps.hasGpu());
    }

    @Test
    void fromJson_null_or_non_object_falls_back_to_defaults() {
        assertEquals(StatefulCaps.defaults(), StatefulCaps.fromJson(null));
        assertEquals(new StatefulCaps(1, 512, 1024, null), StatefulCaps.defaults());
    }

    @Test
    void fromJson_omitted_gpu_stays_null_and_partial_fields_use_defaults() throws Exception {
        StatefulCaps caps = StatefulCaps.fromJson(mapper.readTree("{\"cpu\":4}"));

        assertEquals(4, caps.cpu());
        assertEquals(512, caps.ramMb());
        assertEquals(1024, caps.diskMb());
        assertNull(caps.gpu());
    }

    @Test
    void forUserImage_honors_cpu_and_disk_when_ram_is_set_and_falls_back_per_field() {
        StatefulCaps set = StatefulCaps.forUserImage(2.2, 4096, 8192);
        assertEquals(3, set.cpu());
        assertEquals(4096, set.ramMb());
        assertEquals(8192, set.diskMb());

        StatefulCaps fallback = StatefulCaps.forUserImage(0, 0, 0);
        assertEquals(StatefulCaps.defaults(), fallback);
    }

    @Test
    void fromJson_clamps_non_positive_numeric_fields_to_one() throws Exception {
        // Stateful path clamps (unlike set-warm-target which fails loud) — pin the contract.
        StatefulCaps caps = StatefulCaps.fromJson(mapper.readTree(
                "{\"cpu\":0,\"ramMb\":-5,\"diskMb\":0}"));

        assertEquals(1, caps.cpu());
        assertEquals(1, caps.ramMb());
        assertEquals(1, caps.diskMb());
    }
}
