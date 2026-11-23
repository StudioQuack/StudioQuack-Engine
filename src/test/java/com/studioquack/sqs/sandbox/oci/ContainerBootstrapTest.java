package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The container bootstrap (stdin framing + sh delivery script). */
class ContainerBootstrapTest {

    @Test
    void stdin_payload_frames_script_then_env_as_two_base64_lines() {
        byte[] payload = ContainerBootstrap.stdinPayload("print('x')", "K=v\n");
        String[] lines = new String(payload, StandardCharsets.UTF_8).split("\n", -1);
        assertEquals("print('x')",
                new String(Base64.getDecoder().decode(lines[0]), StandardCharsets.UTF_8));
        assertEquals("K=v\n",
                new String(Base64.getDecoder().decode(lines[1]), StandardCharsets.UTF_8));
    }

    @Test
    void stdin_payload_emits_empty_second_line_when_no_secrets() {
        byte[] payload = ContainerBootstrap.stdinPayload("code", null);
        String s = new String(payload, StandardCharsets.UTF_8);
        String[] lines = s.split("\n", -1);
        assertEquals("", lines[1], "empty .env line when no secrets");
    }

    @Test
    void command_is_a_single_quote_only_plant_and_exec_wrapper() {
        // The bootstrap bears double quotes, so it must not travel as a bare sh -c argv element:
        // the Win32 command-line round-trip on Windows hosts shatters such an argument (see
        // ShExec). The command is a base64 plant-and-exec wrapper instead.
        List<String> cmd = ContainerBootstrap.command(SandboxLanguage.PYTHON);
        assertEquals("sh", cmd.get(0));
        assertEquals("-c", cmd.get(1));
        String wrapper = cmd.get(2);
        assertTrue(wrapper.contains("base64 -d"), "the bootstrap is base64-planted");
        assertTrue(wrapper.contains("&& exec sh "), "the planted script is exec'd");
        assertFalse(wrapper.contains("\""),
                "the wrapper carries no double quotes — survives the Win32 command-line round-trip");
    }

    @Test
    void bootstrap_script_decodes_payload_and_execs_the_pinned_interpreter_over_sandbox_main() {
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.PYTHON);
        assertTrue(bootstrap.contains("base64 -d > '/sandbox/main.py'"), "decodes the script into tmpfs");
        assertTrue(bootstrap.contains("/sandbox/.env"), "loads the per-job .env when present");
        assertTrue(bootstrap.contains("exec 'python3'"), "execs the pinned python interpreter");
        assertTrue(bootstrap.contains("'/sandbox/main.py'"), "runs the sandbox main script");
    }

    @Test
    void node_command_uses_node_interpreter_over_main_js() {
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.JAVASCRIPT);
        assertTrue(bootstrap.contains("'/sandbox/main.js'"));
        assertTrue(bootstrap.contains("exec 'node'"));
    }

    @Test
    void bootstrap_spools_the_payload_and_never_slurps_a_line_with_the_read_builtin() {
        // dash/busybox `read` is one read(2) per byte so it cannot over-consume the pipe: a 21 MB
        // file-bearing script line took ~59 s, past a cold container's own lifetime (exit 137).
        // The payload is spooled once with cat and lines are picked with buffered sed instead.
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.PYTHON);
        assertTrue(bootstrap.contains("cat > '" + ContainerBootstrap.PAYLOAD_SPOOL + "'"),
                "stdin is spooled to the tmpfs in one buffered pass");
        assertTrue(bootstrap.contains("sed -n 1p '" + ContainerBootstrap.PAYLOAD_SPOOL + "' | base64 -d > '/sandbox/main.py'"),
                "the script line is picked with sed, not read");
        assertFalse(bootstrap.contains("read -r"), "no shell read builtin touches the payload");
        int rmSpool = bootstrap.indexOf("rm -f '" + ContainerBootstrap.PAYLOAD_SPOOL + "'");
        int exec = bootstrap.indexOf("exec 'python3'");
        assertTrue(rmSpool > 0 && rmSpool < exec, "the spool is removed before the interpreter starts");
    }
}
