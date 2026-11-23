package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Materialising files into the sandbox tmpfs. Files ride the same stdin channel as the script and
 * the {@code.env}, which is what gives an attachment or a spilled parameter the custody guarantee
 * secrets already have: the bytes reach the container's tmpfs without ever being written to the
 * host filesystem.
 */
class ContainerBootstrapFilesTest {

    private static String payload(String script, String env, List<ContainerBootstrap.SandboxFile> files) {
        return new String(ContainerBootstrap.stdinPayload(script, env, files), StandardCharsets.UTF_8);
    }

    private static String decode(String b64) {
        return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("the payload carries a file count and one name/content pair per file")
    void payload_layout() {
        String out = payload("print(1)", "", List.of(
                new ContainerBootstrap.SandboxFile("a.txt", "AAA".getBytes(StandardCharsets.UTF_8)),
                new ContainerBootstrap.SandboxFile("b.bin", "BBB".getBytes(StandardCharsets.UTF_8))));

        String[] lines = out.split("\n", -1);
        assertEquals("print(1)", decode(lines[0]));
        assertEquals("", lines[1], "no secrets in this job");
        assertEquals("2", lines[2], "line 3 is the file count");
        assertEquals("a.txt", decode(lines[3]));
        assertEquals("AAA", decode(lines[4]));
        assertEquals("b.bin", decode(lines[5]));
        assertEquals("BBB", decode(lines[6]));
    }

    @Test
    @DisplayName("a job with no files still emits the count, so the bootstrap's read is unambiguous")
    void noFiles_emitsZero() {
        String[] lines = payload("x", "ENV=1", List.of()).split("\n", -1);

        assertEquals("0", lines[2]);
    }

    @Test
    @DisplayName("the two-argument form stays wire-compatible")
    void legacyOverload_isUnchangedUpToTheCountLine() {
        String[] lines = new String(ContainerBootstrap.stdinPayload("x", "ENV=1"), StandardCharsets.UTF_8)
                .split("\n", -1);

        assertEquals("x", decode(lines[0]));
        assertEquals("ENV=1", decode(lines[1]));
        assertEquals("0", lines[2]);
    }

    @Test
    @DisplayName("binary content is not corrupted by the encoding")
    void binaryContent_roundTrips() {
        byte[] binary = new byte[256];
        for (int i = 0; i < 256; i++) {
            binary[i] = (byte) i;
        }

        String[] lines = payload("x", "", List.of(
                new ContainerBootstrap.SandboxFile("b.bin", binary))).split("\n", -1);

        assertArrayEquals(binary, Base64.getDecoder().decode(lines[4]));
    }

    @Test
    @DisplayName("each blob is a single line, so the bootstrap's line-based read cannot desynchronise")
    void blobsAreSingleLines() {
        // Java's Base64 encoder is RFC 4648 (no line wrapping); MIME wrapping here would split a blob
        // across lines and the shell loop would read a fragment as the next filename.
        byte[] big = new byte[8192];
        String[] lines = payload("x", "", List.of(
                new ContainerBootstrap.SandboxFile("big.bin", big))).split("\n", -1);

        assertEquals("1", lines[2]);
        assertTrue(lines[4].length() > 8000, "one unwrapped blob line");
        assertEquals(6, lines.length, "script, env, count, name, body, trailing empty");
    }

    @Test
    @DisplayName("the bootstrap clears the files dir before planting, so a warm slot cannot leak")
    void bootstrap_clearsPreviousFiles() {
        // Warm pool slots are reused across nodes; without the reset, one node could read a file the
        // previous node was given.
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.PYTHON);

        assertTrue(bootstrap.contains("rm -rf '" + ContainerBootstrap.FILES_DIR + "'"), bootstrap);
    }

    @Test
    @DisplayName("the bootstrap decodes filenames rather than interpolating them")
    void bootstrap_decodesNamesFromBase64() {
        // Names are sanitised daemon-side; keeping them base64 on the wire additionally means no
        // filename can inject shell syntax into the bootstrap that writes it.
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.PYTHON);

        assertTrue(bootstrap.contains("base64 -d"), bootstrap);
        assertTrue(bootstrap.contains("__SQ_FN="), bootstrap);
        assertTrue(bootstrap.contains("mkdir -p '" + ContainerBootstrap.FILES_DIR + "'"), bootstrap);
    }

    @Test
    @DisplayName("a slashed name is refused before it reaches the payload")
    void slashedName_isRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                ContainerBootstrap.stdinPayload("x", "", List.of(
                        new ContainerBootstrap.SandboxFile("../main.py", "x".getBytes(StandardCharsets.UTF_8)))));
    }

    @Test
    @DisplayName("the bootstrap refuses a decoded name that still contains a slash")
    void bootstrap_refusesSlashedDecodedName() {
        String bootstrap = ContainerBootstrap.bootstrapScript(SandboxLanguage.PYTHON);
        assertTrue(bootstrap.contains("refused slashed name"), bootstrap);
    }

    @Test
    @DisplayName("files land in their own directory, away from main.<ext> and .env")
    void filesDir_isSeparate() {
        assertEquals(HardeningArgs.SANDBOX_DIR + "/files", ContainerBootstrap.FILES_DIR);
    }
}
