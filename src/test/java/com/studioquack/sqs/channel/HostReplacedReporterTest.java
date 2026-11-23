package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HostReplacedReporterTest {

    @Test
    void anIdShapedReplacedByTravelsToTheBackend() {
        assertThat(HostReplacedReporter.requestBody(" 7f0c2a9e-1b2c-4d5e-8f90-123456789abc "))
                .isEqualTo("{\"replacedByEnvironmentId\":\"7f0c2a9e-1b2c-4d5e-8f90-123456789abc\"}");
    }

    @Test
    void anythingElseIsDroppedRatherThanEscaped() {
        assertThat(HostReplacedReporter.requestBody("x\",\"admin\":true")).isEqualTo("{}");
        assertThat(HostReplacedReporter.requestBody(null)).isEqualTo("{}");
        assertThat(HostReplacedReporter.requestBody("")).isEqualTo("{}");
    }
}
