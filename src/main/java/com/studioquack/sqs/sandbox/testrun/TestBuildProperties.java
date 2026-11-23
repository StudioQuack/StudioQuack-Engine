package com.studioquack.sqs.sandbox.testrun;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration surface for daemon-owned test image build+run ({@code POST
 * /v1/helper/test/build-run}). Binds {@code sqs.testbuild.*}.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "sqs.testbuild")
public class TestBuildProperties {

    /**
     * Comma-separated image refs allowed to call test build-run. An entry containing {@code: } is an
     * exact match; an entry without a tag matches that repo with any tag.
     */
    private String allowedImages = "sq-local/sq-builtin-node-library,sq-local/node-test-emulator";

    /** Wall-clock budget (seconds) for {@code podman build} of the staged test context. */
    private int buildWallClockSeconds = 600;

    /** Cap on the per-run wait/invoke timeout the caller may request (seconds). */
    private int maxRunTimeoutSeconds = 600;

    /** Memory cap for the test container ({@code 512m} → 512 MiB). */
    private String runMemory = "512m";

    /** CPU cap forwarded to {@code --cpus}. */
    private double runCpus = 1.0;

    /** Truncation ceiling for build_log / stdout / stderr in the 200 envelope. */
    private int maxLogChars = 20_000;

    /** In-process concurrency cap (tryAcquire; overflow returns a 200 saturated envelope). */
    private int maxConcurrent = 2;
}
