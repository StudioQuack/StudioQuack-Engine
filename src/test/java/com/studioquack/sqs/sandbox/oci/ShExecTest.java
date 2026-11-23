package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link ShExec}: the Windows-immune script transports. The invariant under test — no argv
 * element a script travels in may carry a double quote, because the Win32 command-line round-trip
 * ({@code ProcessBuilder} → {@code CreateProcess} → the Go remote client's re-parse) shatters
 * quote-bearing elements before they reach the container.
 */
class ShExecTest {

    @Test
    void stdinArgv_is_quote_free_and_targets_the_container() {
        List<String> argv = ShExec.stdinArgv("podman", "cid-1");

        assertEquals(List.of("podman", "exec", "-i", "cid-1", "sh", "-s"), argv);
        assertTrue(argv.stream().noneMatch(a -> a.contains("\"")),
                "nothing in the argv can be shattered by the Win32 round-trip");
    }

    @Test
    void utf8_encodes_the_script_bytes() {
        assertEquals("echo héllo", new String(ShExec.utf8("echo héllo"), StandardCharsets.UTF_8));
    }

    @Test
    void plantAndRun_is_single_quote_only_and_propagates_the_exit_code() {
        String script = "echo \"hi\"; exit 3";
        String wrapper = ShExec.plantAndRun(script, "sq_launcher");

        assertFalse(wrapper.contains("\""),
                "no double quotes — survives the Win32 command-line round-trip");
        assertTrue(wrapper.startsWith("printf %s '"), "base64 plant via printf");
        assertTrue(wrapper.contains("| base64 -d > /tmp/sq_launcher.$$.sh"),
                "per-exec plant path ($$ = wrapper PID, no cross-exec collision)");
        assertTrue(wrapper.contains("rc=$?"), "script exit code captured");
        assertTrue(wrapper.contains("rm -f /tmp/sq_launcher.$$.sh"), "plant cleaned up");
        assertTrue(wrapper.endsWith("exit $rc"), "script exit code propagates");
        assertEquals(script, decode(wrapper), "the planted payload is the verbatim script");
    }

    @Test
    void plantAndExec_execs_the_planted_script_without_a_cleanup_tail() {
        String wrapper = ShExec.plantAndExec("echo \"hi\"", "sq_bootstrap");

        assertFalse(wrapper.contains("\""));
        assertTrue(wrapper.contains("&& exec sh /tmp/sq_bootstrap.$$.sh"),
                "single-use job containers exec the planted script");
        assertEquals("echo \"hi\"", decode(wrapper));
    }

    @Test
    void planted_payload_round_trips_arbitrary_shell() {
        // Quotes, dollars, backticks, newlines — base64 makes the payload opaque to every parser
        // between the daemon and the in-container sh.
        String nasty = "if [ \"$(date +%s)\" -ge $SQ_END ]; then echo `pwd` >> \"a b\"; exit 1; fi\n";
        assertEquals(nasty, decode(ShExec.plantAndRun(nasty, "sq_n")));
    }

    /** The first single-quoted span of a wrapper is the base64 payload; decode it. */
    private static String decode(String wrapper) {
        int s = wrapper.indexOf('\'');
        int e = wrapper.indexOf('\'', s + 1);
        return new String(Base64.getDecoder().decode(wrapper.substring(s + 1, e)),
                StandardCharsets.UTF_8);
    }
}
