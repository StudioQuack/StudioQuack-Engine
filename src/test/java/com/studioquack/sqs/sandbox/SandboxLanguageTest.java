package com.studioquack.sqs.sandbox;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the Node runtime contract: the TypeScript entrypoint strips types with Node's built-in
 * {@code --experimental-strip-types}, so its image must be a Node major that knows the flag
 * (22.6+), and both Node defaults are exact patch tags so a host cannot keep an unpatched build.
 */
class SandboxLanguageTest {

    private static final Pattern NODE_MAJOR = Pattern.compile("^node:(\\d+)");
    private static final Pattern EXACT_NODE_TAG = Pattern.compile("^node:\\d+\\.\\d+\\.\\d+-(trixie-)?slim$");

    @Test
    void typescript_entrypoint_strips_types_natively() {
        List<String> argv = SandboxLanguage.TYPESCRIPT.entrypoint("/sandbox");
        assertEquals("node", argv.get(0));
        assertTrue(argv.contains("--experimental-strip-types"));
        assertEquals("/sandbox/main.ts", argv.get(argv.size() - 1));
    }

    @Test
    void typescript_default_image_supports_type_stripping() {
        Matcher m = NODE_MAJOR.matcher(SandboxLanguage.TYPESCRIPT.defaultImage());
        assertTrue(m.find(), "typescript default image must be an official node tag");
        assertTrue(Integer.parseInt(m.group(1)) >= 22,
                "--experimental-strip-types exists only from Node 22.6");
    }

    @Test
    void node_defaults_are_exact_patch_tags() {
        for (SandboxLanguage lang : List.of(SandboxLanguage.JAVASCRIPT, SandboxLanguage.TYPESCRIPT)) {
            assertTrue(EXACT_NODE_TAG.matcher(lang.defaultImage()).matches(),
                    lang + " default must pin an exact Node release, got " + lang.defaultImage());
        }
    }

    /** The warm pool and the image→script-language map are keyed by image ref. */
    @Test
    void javascript_and_typescript_do_not_share_a_default_image() {
        assertNotEquals(SandboxLanguage.JAVASCRIPT.defaultImage(),
                SandboxLanguage.TYPESCRIPT.defaultImage());
    }

    @Test
    void fromWire_accepts_typescript_aliases() {
        assertEquals(SandboxLanguage.TYPESCRIPT, SandboxLanguage.fromWire("typescript"));
        assertEquals(SandboxLanguage.TYPESCRIPT, SandboxLanguage.fromWire("TS"));
    }
}
