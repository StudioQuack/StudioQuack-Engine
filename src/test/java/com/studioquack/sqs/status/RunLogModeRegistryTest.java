package com.studioquack.sqs.status;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * First-writer-wins, fail-open STREAM, and bounded eviction for {@link RunLogModeRegistry}.
 */
class RunLogModeRegistryTest {

    @Test
    void registerIfAbsent_firstWriterWins() {
        RunLogModeRegistry registry = new RunLogModeRegistry();
        assertThat(registry.registerIfAbsent("run-1", RunLogMode.STREAM)).isTrue();
        assertThat(registry.registerIfAbsent("run-1", RunLogMode.BATCH)).isFalse();
        assertThat(registry.modeOf("run-1")).isEqualTo(RunLogMode.STREAM);
    }

    @Test
    void unknownRun_failsOpenToStream() {
        RunLogModeRegistry registry = new RunLogModeRegistry();
        assertThat(registry.modeOf("missing")).isEqualTo(RunLogMode.STREAM);
        assertThat(registry.isBatch("missing")).isFalse();
        assertThat(registry.isBatch(null)).isFalse();
        assertThat(registry.isBatch("")).isFalse();
    }

    @Test
    void isBatch_onlyWhenExplicitlyRegistered() {
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("batch-run", RunLogMode.BATCH);
        assertThat(registry.isBatch("batch-run")).isTrue();
        registry.remove("batch-run");
        assertThat(registry.isBatch("batch-run")).isFalse();
        assertThat(registry.size()).isZero();
    }

    @Test
    void blankOrNull_isIgnored() {
        RunLogModeRegistry registry = new RunLogModeRegistry();
        assertThat(registry.registerIfAbsent(null, RunLogMode.BATCH)).isFalse();
        assertThat(registry.registerIfAbsent("  ", RunLogMode.BATCH)).isFalse();
        assertThat(registry.registerIfAbsent("run", null)).isFalse();
        assertThat(registry.size()).isZero();
    }
}
