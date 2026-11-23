package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RUN flag policy, continuations, escape directive, heredocs. */
class ContainerfilePolicyTest {

    @Test
    void allows_cache_and_tmpfs_mounts_and_plain_run() {
        assertDoesNotThrow(() -> ContainerfilePolicy.validate("""
                FROM scratch
                RUN --mount=type=cache,target=/root/.cache pip install
                RUN --mount=type=tmpfs,target=/tmp echo ok
                RUN echo --network=host
                """));
    }

    @Test
    void rejects_network_security_device_unknown_and_bind() {
        assertLine("RUN --network=host echo", "not allowed");
        assertLine("RUN --network none echo", "not allowed");
        assertLine("RUN --security=insecure echo", "not allowed");
        assertLine("RUN --device=/dev/fuse echo", "not allowed");
        assertLine("RUN --privileged echo", "not allowed");
        assertLine("RUN --mount=type=bind,source=/etc,target=/host echo", "not allowed");
        assertLine("RUN --mount=type=secret,id=x echo", "not allowed");
        assertLine("RUN --mount=type=ssh echo", "not allowed");
        assertLine("RUN --mount=type=cache,source=/var/cache,target=/c echo", "source, src, or from");
        assertLine("RUN --mount=type=cache,src=/var/cache,target=/c echo", "source, src, or from");
        assertLine("RUN --mount=type=cache,from=other,target=/c echo", "source, src, or from");
    }

    @Test
    void continuations_escape_directive_and_case_are_honored() {
        assertLine("""
                # syntax=docker/dockerfile:1
                # escape=`
                RUN --network=host `
                    echo hi
                """, "not allowed");
        assertDoesNotThrow(() -> ContainerfilePolicy.validate("""
                # escape=`
                RUN --mount=type=cache,target=/c `
                    echo hi
                """));
        assertLine("run --network=none true", "not allowed");
        assertLine("OnBuild RuN --network=host /bin/true", "not allowed");
    }

    @Test
    void escape_directive_after_a_comment_does_not_apply() {
        // A real comment closes the directive window, so backtick is not the escape
        // and the following line is a separate instruction that carries the flag.
        assertLine("""
                # hello
                # escape=`
                RUN --network=host echo
                """, "not allowed");
    }

    @Test
    void heredoc_body_is_not_parsed_and_unterminated_is_rejected() {
        assertDoesNotThrow(() -> ContainerfilePolicy.validate("""
                RUN <<EOF
                RUN --network=host echo
                EOF
                RUN cat <<-'WORD'
                --mount=type=bind,source=/
                WORD
                """));
        ContainerfilePolicy.Violation v = assertThrows(ContainerfilePolicy.Violation.class,
                () -> ContainerfilePolicy.validate("RUN <<EOF\necho hi\n"));
        assertTrue(v.getMessage().contains("unterminated heredoc"), v.getMessage());
        assertTrue(v.getMessage().startsWith("line "), v.getMessage());
    }

    @Test
    void exec_form_flags_are_arguments() {
        assertDoesNotThrow(() -> ContainerfilePolicy.validate(
                "RUN [\"echo\", \"--network=host\"]\n"));
    }

    private static void assertLine(String file, String reason) {
        ContainerfilePolicy.Violation v = assertThrows(ContainerfilePolicy.Violation.class,
                () -> ContainerfilePolicy.validate(file.startsWith("FROM") || file.startsWith("#")
                        || file.startsWith("run") || file.startsWith("OnBuild") || file.startsWith("RUN")
                        ? (file.contains("FROM") || file.startsWith("#") || file.startsWith("run")
                        || file.startsWith("OnBuild") ? file : "FROM scratch\n" + file)
                        : "FROM scratch\n" + file));
        assertTrue(v.getMessage().contains(reason), v.getMessage());
        assertTrue(v.getMessage().contains("line "), v.getMessage());
        assertTrue(v.getMessage().length() < 200, v.getMessage());
    }
}
