package com.studioquack.sqs.sandbox.egress;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Spring must build the bean through the configured constructor. With two constructors and no
 * {@code @Autowired}, Spring silently picks the no-arg test seam — mode OFF, channel disabled —
 * and every daemon runs with egress enforcement switched off while reporting an opt-out.
 */
class SandboxEgressManagerWiringTest {

    @Test
    void spring_uses_the_configured_constructor_not_the_test_seam() {
        Constructor<?>[] picked = new AutowiredAnnotationBeanPostProcessor()
                .determineCandidateConstructors(SandboxEgressManager.class, "sandboxEgressManager");
        assertNotNull(picked, "no constructor marked for injection: Spring would use the no-arg seam");
        assertEquals(1, picked.length);
        assertEquals(7, picked[0].getParameterCount());
    }
}
