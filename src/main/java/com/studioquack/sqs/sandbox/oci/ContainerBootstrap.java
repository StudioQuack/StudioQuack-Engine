package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Builds the in-container bootstrap that plants the (helper-prepended, context-baked) node script
 * — and, when present, the per-job {@code .env} of resolved secrets — into the {@code --tmpfs
 * /sandbox} scratch and then {@code exec}s the pinned interpreter. The rootfs is {@code
 * --read-only} and the sandbox dir is an in-container tmpfs; nothing the script or secrets touch
 * is ever written to the host filesystem.
 */
public final class ContainerBootstrap {

    private ContainerBootstrap() {
    }

    /**
     * The container command ({@code sh -c <bootstrap>}) that reads the stdin payload and execs the
     * interpreter. The interpreter argv is the language's pinned entrypoint over
     * {@code /sandbox/main.<ext>}.
     */
    /** Where materialised parameter/attachment files land, inside the tmpfs and beside nothing else. */
    public static final String FILES_DIR = HardeningArgs.SANDBOX_DIR + "/files";

    public static List<String> command(SandboxLanguage language) {
        // The bootstrap bears double quotes, so it must not travel as a bare sh -c argv element: on
        // Windows hosts the Win32 command-line round-trip shatters such an argument before it reaches the
        // container. It is base64-planted and exec'd as a file instead — stdin stays free for the payload,
        // and the wrapper is single-quote-only (Windows-safe).
        return List.of("sh", "-c", ShExec.plantAndExec(bootstrapScript(language), "sq_bootstrap"));
    }

    /** Spool path for the stdin payload inside the tmpfs; removed before the interpreter starts. */
    static final String PAYLOAD_SPOOL = HardeningArgs.SANDBOX_DIR + "/.sq_payload";

    /** The raw bootstrap the wrapper plants: read the stdin payload, then exec the interpreter. */
    static String bootstrapScript(SandboxLanguage language) {
        String main = HardeningArgs.SANDBOX_DIR + "/" + language.scriptFileName();
        String execLine = "exec " + joinArgv(language.entrypoint(HardeningArgs.SANDBOX_DIR));
        String spool = shSingleQuote(PAYLOAD_SPOOL);
        // POSIX sh: line 1 → main.<ext>, optional line 2 → .env (sourced into the env), then line 3 = file
        // count followed by that many (name, content) base64 line pairs. The payload is spooled to the
        // tmpfs with `cat` and each line is then picked with `sed -n Np` — never with the shell's `read`
        // builtin. `read` must not over-consume the pipe, so dash/busybox implement it as one read(2)
        // syscall PER BYTE: a 21 MB file-bearing script line took ~59 s to slurp, long enough for a cold
        // container's own lifetime to end and SIGKILL the exec (exit 137) before the interpreter ever
        // started. `cat` and `sed` are buffered and finish the same payload in well under a second, and no
        // multi-MB line ever sits in a shell variable.
        return String.join("\n",
                "set -e",
                "rm -f " + shSingleQuote(HardeningArgs.SANDBOX_DIR + "/.env") + " 2>/dev/null || true",
                "rm -rf " + shSingleQuote(FILES_DIR) + " 2>/dev/null || true",
                "cat > " + spool,
                "sed -n 1p " + spool + " | base64 -d > " + shSingleQuote(main),
                "__SQ_ENV=$(sed -n 2p " + spool + ")",
                "if [ -n \"$__SQ_ENV\" ]; then",
                "  printf %s \"$__SQ_ENV\" | base64 -d > " + shSingleQuote(HardeningArgs.SANDBOX_DIR + "/.env"),
                "  set -a",
                "  . " + shSingleQuote(HardeningArgs.SANDBOX_DIR + "/.env"),
                "  set +a",
                "fi",
                // Files are optional: an older payload simply ends after the .env line (line 3 empty).
                "__SQ_NFILES=$(sed -n 3p " + spool + ")",
                "if [ -n \"$__SQ_NFILES\" ] && [ \"$__SQ_NFILES\" -gt 0 ] 2>/dev/null; then",
                "  mkdir -p " + shSingleQuote(FILES_DIR),
                "  __SQ_I=0",
                "  while [ \"$__SQ_I\" -lt \"$__SQ_NFILES\" ]; do",
                // The name is decoded into a shell variable and used only as a basename under
                // FILES_DIR. It is sanitised daemon-side before encoding; keeping it base64 on the
                // wire additionally means no filename can inject shell syntax into this bootstrap.
                "    __SQ_FN=$(sed -n \"$((4 + __SQ_I * 2))p\" " + spool + " | base64 -d)",
                "    if [ -z \"$__SQ_FN\" ]; then echo 'sandbox.files: empty name' >&2; exit 1; fi",
                "    case \"$__SQ_FN\" in */*) echo 'sandbox.files: refused slashed name' >&2; exit 1 ;; esac",
                "    sed -n \"$((5 + __SQ_I * 2))p\" " + spool + " | base64 -d > "
                        + shSingleQuote(FILES_DIR) + "/\"$__SQ_FN\"",
                "    __SQ_I=$((__SQ_I + 1))",
                "  done",
                "fi",
                "rm -f " + spool,
                execLine);
    }

    /**
     * The bytes piped to the container's stdin: {@code base64(script)\n} then
     * {@code base64(envFileContent-or-empty)\n}. Java's {@link Base64#getEncoder()} emits no
     * newlines (RFC 4648, not MIME) so each blob is a single line the bootstrap's {@code read}
     * consumes cleanly.
     */
    public static byte[] stdinPayload(String scriptSource, String envFileContent) {
        return stdinPayload(scriptSource, envFileContent, List.of());
    }

    /** One file to plant in {@link #FILES_DIR}; {@code name} must already be a sanitised basename. */
    public record SandboxFile(String name, byte[] content) { }

    /**
     * As {@link #stdinPayload(String, String)}, plus a third section carrying files: {@code
     * base64(script)\n base64(env)\n <count>\n} then {@code base64(name)\n base64(content)\n} per
     * file. The same stdin channel as the script and secrets, for the same reason: the payload reaches
     * the container's tmpfs without ever being written to the host, so an attachment or a spilled
     * parameter gets exactly the custody guarantee secrets already have.
     */
    public static byte[] stdinPayload(String scriptSource, String envFileContent, List<SandboxFile> files) {
        String scriptB64 = Base64.getEncoder().encodeToString(
                (scriptSource == null ? "" : scriptSource).getBytes(StandardCharsets.UTF_8));
        String envB64 = (envFileContent == null || envFileContent.isEmpty())
                ? ""
                : Base64.getEncoder().encodeToString(envFileContent.getBytes(StandardCharsets.UTF_8));

        StringBuilder sb = new StringBuilder()
                .append(scriptB64).append('\n')
                .append(envB64).append('\n');

        List<SandboxFile> safe = files == null ? List.of() : files;
        sb.append(safe.size()).append('\n');
        for (SandboxFile file : safe) {
            requireBasename(file.name());
            sb.append(Base64.getEncoder().encodeToString(
                    file.name().getBytes(StandardCharsets.UTF_8))).append('\n');
            sb.append(Base64.getEncoder().encodeToString(file.content())).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Joins an interpreter argv into a single {@code sh}-safe command line (each token quoted). */
    private static String joinArgv(List<String> argv) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < argv.size(); i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(shSingleQuote(argv.get(i)));
        }
        return sb.toString();
    }

    /** Single-quotes a token for POSIX sh, escaping embedded single quotes. */
    private static String shSingleQuote(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }

    /** Second layer: a planted name must be a basename (materializer already ran {@code safeName}). */
    static void requireBasename(String name) {
        if (name == null || name.isBlank() || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("sandbox file name must be a sanitised basename");
        }
    }
}
