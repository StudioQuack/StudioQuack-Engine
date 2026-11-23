package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Daemon path + buildId validation (IllegalArgumentException, not a remote validation error). */
class OciBuildPathValidatorTest {

    @Test
    void normalisesRepeatedSlashes() {
        assertEquals("a/b", OciBuildPathValidator.normalizeAndValidate("a//b"));
        assertEquals("src/main/java/Index.java",
                OciBuildPathValidator.normalizeAndValidate("src//main/java//Index.java"));
    }

    @Test
    void rejectsPathTraversalAndAbsolute() {
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("../x"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("a/../b"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("/abs"));
        IllegalArgumentException abs = assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("/abs/path"));
        assertTrue(abs.getMessage().toLowerCase().contains("relative"));
    }

    @Test
    void rejectsBackslashDotEmptyAndTrailingSlash() {
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("a\\b"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("a/./b"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate(""));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("   "));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("a/b/"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.normalizeAndValidate("a//"));
    }

    @Test
    void requireBuildId_acceptsUuidSafe() {
        assertEquals("550e8400-e29b-41d4-a716-446655440000",
                OciBuildPathValidator.requireBuildId("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals("build_1.2", OciBuildPathValidator.requireBuildId(" build_1.2 "));
    }

    @Test
    void requireBuildId_rejectsUnsafe() {
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId(""));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId("a/b"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId("a\\b"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId(".."));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId("bad id"));
        assertThrows(IllegalArgumentException.class,
                () -> OciBuildPathValidator.requireBuildId("id;rm"));
    }
}
