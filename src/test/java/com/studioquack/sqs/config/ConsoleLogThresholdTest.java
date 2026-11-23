package com.studioquack.sqs.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultBootstrapContext;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessorsFactory;
import org.springframework.boot.logging.DeferredLogs;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A supervised engine's console is appended to a file nothing rotates (Windows scheduled task,
 * macOS LaunchAgent) — it must not repeat what the rotated log file already holds.
 */
class ConsoleLogThresholdTest {

    private static final String LOG = "C:\\Users\\Mert\\AppData\\Local\\StudioQuack\\sqs\\data\\logs\\daemon.log";

    @Test
    void aSupervisedEngineWithALogFileLogsNothingToTheConsole() {
        assertTrue(ConsoleLogThreshold.silenceConsole(null, LOG, Map.of(), false));
    }

    @Test
    void systemdJournalKeepsTheConsole() {
        assertFalse(ConsoleLogThreshold.silenceConsole(null, LOG, Map.of("JOURNAL_STREAM", "8:12345"), false));
    }

    @Test
    void aForegroundRunOrNoLogFileKeepsTheConsole() {
        assertFalse(ConsoleLogThreshold.silenceConsole(null, LOG, Map.of(), true));
        assertFalse(ConsoleLogThreshold.silenceConsole(null, null, Map.of(), false));
        assertFalse(ConsoleLogThreshold.silenceConsole(null, " ", Map.of(), false));
    }

    @Test
    void anExplicitThresholdWins() {
        assertFalse(ConsoleLogThreshold.silenceConsole("INFO", LOG, Map.of(), false));
    }

    @Test
    void addsTheThresholdAsALowPrioritySource() {
        MockEnvironment env = new MockEnvironment().withProperty("logging.file.name", LOG);

        new ConsoleLogThreshold().postProcessEnvironment(env, null);

        // The test JVM has no terminal and (on CI) no journal: the console is silenced.
        if (System.getenv("JOURNAL_STREAM") == null) {
            assertEquals("OFF", env.getProperty(ConsoleLogThreshold.PROPERTY));
        }
    }

    @Test
    void springBootLoadsBothEnvironmentPostProcessorsCleanly() {
        List<EnvironmentPostProcessor> loaded = EnvironmentPostProcessorsFactory
                .fromSpringFactories(ConsoleLogThreshold.class.getClassLoader())
                .getEnvironmentPostProcessors(new DeferredLogs(), new DefaultBootstrapContext());
        List<String> names = loaded.stream().map(p -> p.getClass().getName()).toList();
        assertTrue(names.contains("com.studioquack.sqs.config.ConsoleLogThreshold"),
                "Spring Boot must discover ConsoleLogThreshold: " + names);
        assertTrue(names.contains("com.studioquack.sqs.secrets.os.H2CipherPasswordEnvironmentPostProcessor"),
                "Spring Boot must discover H2CipherPasswordEnvironmentPostProcessor: " + names);
    }
}
