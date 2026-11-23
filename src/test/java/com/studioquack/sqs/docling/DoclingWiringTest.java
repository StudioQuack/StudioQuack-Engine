package com.studioquack.sqs.docling;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Docling conversion is always local. Whether a node may run is a per-environment entitlement
 * checked at execution.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-docling-wiring-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.docling.url=http://docling.test.invalid:5001"
})
@ActiveProfiles("base")
class DoclingWiringTest {

    @Autowired
    private ApplicationContext ctx;
    @Autowired
    private DoclingBackend doclingBackend;

    @Test
    @DisplayName("conversion is local, and it is the only backend")
    void localBackendIsTheOnlyOne() {
        // Conversion stays in the environment.
        assertInstanceOf(LocalDoclingBackend.class, doclingBackend);
        assertEquals(1, ctx.getBeansOfType(DoclingBackend.class).size(),
                "exactly one DoclingBackend must be active");
    }

    @Test
    @DisplayName("the delegate and the side-car client are wired unconditionally")
    void wiringIsUnconditional() {
        // Nothing is gated on a boot property any more; the runtime toggle decides whether a node runs.
        assertTrue(ctx.containsBean("doclingDelegate"));
        assertInstanceOf(DoclingDelegate.class, ctx.getBean("doclingDelegate"));
        assertEquals(1, ctx.getBeansOfType(DoclingClient.class).size());
        assertEquals(1, ctx.getBeansOfType(DoclingContainer.class).size(),
                "the on-demand container manager is always available");
    }
}
