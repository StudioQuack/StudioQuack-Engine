package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.MaintenanceProperties;
import com.studioquack.sqs.maintenance.MaintenanceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The two restored shared command handlers {@code status} and {@code logs}. Proves each reports the
 * right wire {@code type}, produces a well-formed JSON blob, and never throws out of {@code handle}
 * (a provider/service failure becomes a {@code failed} outcome or a safe fallback).
 */
class StatusLogsCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path root;

    private static DaemonCommand cmd(String id, String type, String payload) {
        return new DaemonCommand(id, type, "2026-07-13T00:00:00Z", payload);
    }

    // ---- status ---------------------------------------------------------

    @Test
    void status_type_isStatus() {
        assertEquals("status", new StatusCommandHandler(mapper, () -> Map.of(), "base").type());
    }

    @Test
    void status_returnsResourceJson_fromProvider() throws Exception {
        HeartbeatResourceProvider provider = () -> Map.of(
                "cpuCount", 8,
                "dbSizeBytes", 12_345L,
                "jvmHeapUsedBytes", 999L);
        StatusCommandHandler handler = new StatusCommandHandler(mapper, provider, "base");

        DaemonCommandOutcome outcome = handler.handle(cmd("s1", "status", null));

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        assertEquals("base", body.get("tier"));
        assertEquals("UP", body.get("daemonStatus"));
        assertNotNull(body.get("uptimeSec"));
        assertNotNull(body.get("reportedAt"));
        Map<?, ?> resources = (Map<?, ?>) body.get("resources");
        assertEquals(8, ((Number) resources.get("cpuCount")).intValue());
        assertEquals(12_345, ((Number) resources.get("dbSizeBytes")).intValue());
    }

    @Test
    void status_fallsBackToMinimalJvmSnapshot_whenNoProvider() throws Exception {
        StatusCommandHandler handler = new StatusCommandHandler(mapper, null, "base");

        DaemonCommandOutcome outcome = handler.handle(cmd("s2", "status", null));

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        Map<?, ?> resources = (Map<?, ?>) body.get("resources");
        assertEquals("minimal-fallback", resources.get("resourceProvider"));
        assertNotNull(resources.get("cpuCount"));
        assertNotNull(resources.get("jvmHeapUsedBytes"));
    }

    @Test
    void status_neverThrows_whenProviderThrows_usesFallback() throws Exception {
        HeartbeatResourceProvider bad = () -> {
            throw new RuntimeException("provider exploded");
        };
        StatusCommandHandler handler = new StatusCommandHandler(mapper, bad, "base");

        DaemonCommandOutcome outcome = handler.handle(cmd("s3", "status", null));

        assertTrue(outcome.succeeded(), "a throwing provider must not fail status; it falls back");
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        Map<?, ?> resources = (Map<?, ?>) body.get("resources");
        assertEquals("minimal-fallback", resources.get("resourceProvider"));
    }

    // ---- logs -----------------------------------------------------------

    @Test
    void logs_type_isLogs() {
        assertEquals("logs", new LogsCommandHandler(mapper, mock(MaintenanceService.class)).type());
    }

    @Test
    void logs_tailsNewestDaemonLogFile_boundedByLines() throws Exception {
        Path logDir = Files.createDirectories(root.resolve("logs"));
        Files.writeString(logDir.resolve("app.log"), "line1\nline2\nline3\nline4\n");

        MaintenanceService service = realService(logDir);
        LogsCommandHandler handler = new LogsCommandHandler(mapper, service);

        DaemonCommandOutcome outcome = handler.handle(cmd("l1", "logs", "{\"lines\":2}"));

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        assertEquals("daemon-log-file", body.get("source"));
        assertEquals("app.log", body.get("filename"));
        assertEquals(2, ((Number) body.get("lineCount")).intValue());
        String content = (String) body.get("content");
        assertTrue(content.contains("line4"), "keeps the newest lines");
        assertFalse(content.contains("line1"), "drops the oldest beyond the line cap");
    }

    @Test
    void logs_hostManagedNote_whenNoLogDirConfigured() throws Exception {
        MaintenanceService service = realService(null); // blank log dir

        DaemonCommandOutcome outcome = new LogsCommandHandler(mapper, service).handle(cmd("l2", "logs", null));

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        assertEquals("host-managed", body.get("source"));
        assertTrue(((String) body.get("content")).contains("host-level"));
    }

    @Test
    void logs_neverThrows_whenServiceThrows() {
        MaintenanceService service = mock(MaintenanceService.class);
        when(service.tailLogs(org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyLong()))
                .thenThrow(new RuntimeException("io boom"));

        DaemonCommandOutcome outcome = new LogsCommandHandler(mapper, service).handle(cmd("l3", "logs", null));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("logs failed"));
    }

    private MaintenanceService realService(Path logDir) {
        MaintenanceProperties props = new MaintenanceProperties();
        if (logDir != null) {
            props.setLogDir(logDir.toString());
        }
        return new MaintenanceService(root.toString(), props);
    }
}
