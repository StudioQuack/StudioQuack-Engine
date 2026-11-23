package com.studioquack.sqs.channel;

import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Caller/listener port-drift guard. The cross-env execute endpoint moved off the loopback server
 * precisely because caller ({@code HelperExecuteService.forwardToPeer}) and listener ({@code
 * SqsChannelBootstrap}'s peer listener) once disagreed on where it lived.
 */
class OverlayExecutePortDriftTest {

    /** The single agreed placeholder in application.yml (env override + default). */
    private static final String YAML_PLACEHOLDER = "${SQS_OVERLAY_EXECUTE_PORT:8443}";
    /** The single agreed injection literal on both the caller and the listener side. */
    private static final String VALUE_LITERAL = "${sqs.overlay.execute-port:8443}";

    @Test
    void applicationYml_declaresTheAgreedPlaceholder() {
        Map<String, Object> doc = loadApplicationYml();
        Object sqs = doc.get("sqs");
        assertTrue(sqs instanceof Map, "application.yml has an sqs section");
        Object overlay = ((Map<?, ?>) sqs).get("overlay");
        assertTrue(overlay instanceof Map, "application.yml has an sqs.overlay section");
        Object executePort = ((Map<?, ?>) overlay).get("execute-port");
        assertEquals(YAML_PLACEHOLDER, executePort,
                "sqs.overlay.execute-port placeholder drifted (env var name or default)");
    }

    @Test
    void callerSideValueLiteral_matchesTheAgreedKeyAndDefault() throws Exception {
        // The caller: HelperExecuteService dials http://{peerOverlayIp}:${overlayExecutePort}/...
        java.lang.reflect.Field field = HelperExecuteService.class.getDeclaredField("overlayExecutePort");
        Value value = field.getAnnotation(Value.class);
        assertNotNull(value, "HelperExecuteService.overlayExecutePort lost its @Value injection");
        assertEquals(VALUE_LITERAL, value.value(),
                "the caller-side @Value literal drifted from sqs.overlay.execute-port:8443");
    }

    @Test
    void listenerSideValueLiteral_matchesTheAgreedKeyAndDefault() {
        // The listener: SqsChannelBootstrap binds the peer listener on the injected port.
        Constructor<?> ctor = SqsChannelBootstrap.class.getDeclaredConstructors()[0];
        String found = null;
        for (Parameter param : ctor.getParameters()) {
            Value value = param.getAnnotation(Value.class);
            if (value != null && value.value().contains("execute-port")) {
                found = value.value();
            }
        }
        assertEquals(VALUE_LITERAL, found,
                "the listener-side @Value literal drifted from sqs.overlay.execute-port:8443");
    }

    @Test
    void yamlDefault_andCodeDefault_areTheSamePort() {
        assertEquals(defaultOf(YAML_PLACEHOLDER), defaultOf(VALUE_LITERAL),
                "caller/listener default ports disagree — this is the drift this slice exists to kill");
        assertEquals("8443", defaultOf(VALUE_LITERAL), "the agreed default port is 8443");
    }

    /** Extracts the default after the LAST ':' of a ${KEY:default} / ${ENV:default} placeholder. */
    private static String defaultOf(String placeholder) {
        int colon = placeholder.lastIndexOf(':');
        assertTrue(colon > 0 && placeholder.endsWith("}"), "not a ${...:default} placeholder: " + placeholder);
        return placeholder.substring(colon + 1, placeholder.length() - 1);
    }

    private static Map<String, Object> loadApplicationYml() {
        try (InputStream in = OverlayExecutePortDriftTest.class.getClassLoader()
                .getResourceAsStream("application.yml")) {
            assertNotNull(in, "application.yml must be on the test classpath");
            return new Yaml().load(in);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("could not read application.yml", e);
        }
    }
}
