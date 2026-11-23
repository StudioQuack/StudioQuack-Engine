package com.studioquack.sqs.engine.bpmn;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeelToJuelTranslatorTest {

    private final FeelToJuelTranslator translator = new FeelToJuelTranslator();

    @Test
    void feel_route_compare_becomes_safe_juel() {
        assertEquals("${route__gw1 == 'approved'}",
                translator.translateCondition("= route__gw1 = 'approved'"));
        assertTrue(FeelToJuelTranslator.isSafeRouteCompare("${route__gw1 == 'approved'}"));
    }

    @Test
    void already_safe_juel_passes_through() {
        assertEquals("${route__gw1 == 'approved'}",
                translator.translateCondition("${route__gw1 == 'approved'}"));
    }

    @Test
    void bean_method_juel_is_rejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translator.translateCondition("${channelProperties.getBearer()}"));
        assertTrue(ex.getMessage().contains("route__"));
        assertThrows(IllegalArgumentException.class,
                () -> translator.translateCondition("${osSecretManager.get('x')}"));
        assertThrows(IllegalArgumentException.class,
                () -> translator.translateCondition("${daemonProcessRunner.run(['/bin/sh'], 1)}"));
    }

    @Test
    void el_injection_shapes_are_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> translator.translateCondition("${T(java.lang.Runtime).getRuntime().exec('x')}"));
        assertThrows(IllegalArgumentException.class,
                () -> translator.translateCondition("${channelProperties.bearer}"));
    }

    @Test
    void blank_conditions_yield_no_expression() {
        assertNull(translator.translateCondition("="));
        assertNull(translator.translateCondition("   "));
        assertNull(translator.translateCondition(null));
    }

    @Test
    void non_route_conditions_fail_instead_of_silently_dropping() {
        // A dropped condition leaves the edge unconditioned (always taken) — that is a misroute,
        // not a fallback. Plain variables, garbage and bare literals all fail the deploy.
        for (String bad : new String[] {"${approved}", "${!approved}", "${unclosed == 'x'",
                "= status = 'ok'", "${route__gw1 == other}", "${route__gw1 == 'a'} || ${x}"}) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> translator.translateCondition(bad), bad);
            assertTrue(ex.getMessage().contains("route__"), ex.getMessage());
        }
    }

    @Test
    void literal_true_false_pass() {
        assertEquals("${true}", translator.translateCondition("${true}"));
        assertEquals("${false}", translator.translateCondition("= false"));
    }

    @Test
    void lone_equals_inside_a_quoted_literal_is_never_rewritten() {
        // The route literal 'a=b' must survive verbatim — a blanket regex would corrupt it
        // to 'a==b' and the safe-grammar gate would bless the corrupted text.
        assertEquals("${route__gw1 == 'a=b'}",
                translator.translateCondition("= route__gw1 = 'a=b'"));
        // An already-${...} string passes through untouched — literals inside it too.
        assertEquals("${route__gw1 == 'x=y=z'}",
                translator.translateCondition("${route__gw1 == 'x=y=z'}"));
        // An escaped quote inside the literal does not end it early.
        assertEquals("${route__gw1 == 'it\\'s=a'}",
                translator.translateCondition("= route__gw1 = 'it\\'s=a'"));
    }
}
