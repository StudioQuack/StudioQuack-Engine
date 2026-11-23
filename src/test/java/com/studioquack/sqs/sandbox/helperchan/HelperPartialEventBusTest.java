package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HelperPartialEventBusTest {

    @Test
    void emitWithoutParentReturnsFalse() {
        HelperPartialEventBus bus = new HelperPartialEventBus();
        assertFalse(bus.emit("child-1", "answer", "x"));
    }

    @Test
    void emitDeliversToRegisteredParent() {
        HelperPartialEventBus bus = new HelperPartialEventBus();
        List<String> seen = new ArrayList<>();
        bus.register("child-1", (channel, payload) -> seen.add(channel + ":" + payload));
        assertTrue(bus.emit("child-1", "thinking", "tok"));
        assertEquals(List.of("thinking:tok"), seen);
        bus.unregister("child-1");
        assertFalse(bus.emit("child-1", "answer", "late"));
    }

    @Test
    void everyEmitDeliversSynchronously_withNoBufferedCopy() {
        HelperPartialEventBus bus = new HelperPartialEventBus();
        AtomicInteger delivered = new AtomicInteger();
        bus.register("child-1", (channel, payload) -> delivered.incrementAndGet());
        // Well past the old queue cap: delivery must never degrade into drop-oldest,
        // because the listener is the only consumer and gets every frame live.
        int frames = HelperStreamLimits.MAX_QUEUED_FRAMES * 2;
        for (int i = 0; i < frames; i++) {
            assertTrue(bus.emit("child-1", "answer", i));
        }
        assertEquals(frames, delivered.get());
    }
}
