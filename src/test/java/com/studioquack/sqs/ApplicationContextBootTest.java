package com.studioquack.sqs;

import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Boots the full application context against an in-memory datasource. Exists because of a real regression. */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-boot-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class ApplicationContextBootTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void contextBootsAndBuildContextStoreWires() {
        assertNotNull(context, "application context failed to boot");
        // The bean that previously failed to instantiate. If the wiring regresses this throws
        // NoSuchBeanDefinitionException / BeanCreationException rather than passing silently.
        assertNotNull(context.getBean(OciBuildContextStore.class),
                "ociBuildContextStore failed to wire — check for a missing @Autowired on a "
                        + "multi-constructor bean");
    }
}
