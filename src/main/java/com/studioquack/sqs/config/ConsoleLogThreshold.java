package com.studioquack.sqs.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.io.Console;
import java.util.Map;

/**
 * Keeps a supervised engine's console from duplicating its log file. A Windows scheduled task and
 * a macOS LaunchAgent append the console to a file nothing rotates, while the same lines already
 * go to the bounded, rotated {@code LOGGING_FILE_NAME}.
 */
public class ConsoleLogThreshold implements EnvironmentPostProcessor {

    static final String PROPERTY = "logging.threshold.console";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Console console = System.console();
        String logFile = environment.getProperty("logging.file.name");
        if (silenceConsole(environment.getProperty(PROPERTY), logFile, System.getenv(),
                console != null && console.isTerminal())) {
            environment.getPropertySources().addLast(
                    new MapPropertySource("sqsConsoleLogThreshold", Map.of(PROPERTY, "OFF")));
            // An operator reading this console capture must not take the silence for a dead engine.
            System.out.println(pointer(logFile));
        }
    }

    static String pointer(String logFile) {
        return "==> log lines are written to " + logFile
                + " (rotated, size-capped); this console capture keeps only start-up banners and JVM crash output";
    }

    static boolean silenceConsole(String explicitThreshold, String logFile, Map<String, String> env,
                                  boolean interactive) {
        if (explicitThreshold != null && !explicitThreshold.isBlank()) {
            return false;
        }
        if (logFile == null || logFile.isBlank()) {
            return false; // the console is the only log
        }
        if (interactive) {
            return false; // a foreground run: the operator is watching
        }
        return !env.containsKey("JOURNAL_STREAM");
    }
}
