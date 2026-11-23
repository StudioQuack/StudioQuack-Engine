package com.studioquack.sqs.engine.runtime;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * UUID minted once per JVM boot and stamped on function-port sub-processes.
 * Dispatch rejects an activation whose epoch does not match this boot.
 */
@Slf4j
@Component
public class BeRuntimeEpoch {

    /** Reserved engine variable name carrying the daemon's startup epoch. */
    public static final String VARIABLE_NAME = "__sq_be_epoch";

    private final String epoch = UUID.randomUUID().toString();

    @PostConstruct
    void announce() {
        log.info("BeRuntimeEpoch initialised epoch={} — sub-PI activations from previous daemon "
                + "runs will be rejected on first activation.", epoch);
    }

    /** @return this daemon process's epoch. Stable for the JVM lifetime; mints fresh on every boot. */
    public String current() {
        return epoch;
    }

    /**
     * Predicate used by the dispatch path to decide whether an activated sub-PI job belongs
     * to this daemon boot. Null / blank arguments return {@code false} so a legacy sub-PI
     * created before this field existed is conservatively treated as orphaned (worst case:
     * one wasted dispatch, never a dropped real job — real PIs always carry a non-blank epoch).
     */
    public boolean matches(String stamped) {
        return stamped != null && !stamped.isEmpty() && epoch.equals(stamped);
    }
}
