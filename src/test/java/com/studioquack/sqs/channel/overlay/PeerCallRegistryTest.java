package com.studioquack.sqs.channel.overlay;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Only the environment that placed a call can cancel it, once; finished calls are gone. */
class PeerCallRegistryTest {

    private final PeerCallRegistry registry = new PeerCallRegistry();
    private final AtomicInteger aborts = new AtomicInteger();

    @Test
    void theCallerCancelsItsCallOnce() {
        assertTrue(registry.open("c1", "env-a", PeerCallAuthenticator.SCOPE_LLM, aborts::incrementAndGet));
        assertEquals(PeerCallAuthenticator.SCOPE_LLM, registry.scopeOf("c1"));

        assertTrue(registry.cancel("c1", "env-a"));
        assertFalse(registry.cancel("c1", "env-a"));
        assertEquals(1, aborts.get());
        assertNull(registry.scopeOf("c1"));
    }

    @Test
    void anotherEnvironmentCannotCancel() {
        registry.open("c2", "env-a", PeerCallAuthenticator.SCOPE_EXECUTE, aborts::incrementAndGet);
        assertFalse(registry.cancel("c2", "env-b"));
        assertEquals(0, aborts.get());
        assertEquals(1, registry.size());
    }

    @Test
    void aClosedCallCannotBeCancelled() {
        registry.open("c3", "env-a", PeerCallAuthenticator.SCOPE_EXECUTE, aborts::incrementAndGet);
        registry.close("c3");
        assertFalse(registry.cancel("c3", "env-a"));
        assertEquals(0, aborts.get());
    }

    @Test
    void aBlankOrReusedIdIsServedWithoutCancelSupport() {
        assertFalse(registry.open(" ", "env-a", PeerCallAuthenticator.SCOPE_LLM, aborts::incrementAndGet));
        assertTrue(registry.open("c4", "env-a", PeerCallAuthenticator.SCOPE_LLM, aborts::incrementAndGet));
        assertFalse(registry.open("c4", "env-b", PeerCallAuthenticator.SCOPE_LLM, aborts::incrementAndGet),
                "a second caller must not replace the first one's entry");
    }
}
