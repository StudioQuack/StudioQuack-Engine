package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.oci.ShExec;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Handles the BE-pushed {@code stateful-instance-list-files} command: lists regular files under
 * {@code /data/sq-bespoke/<templateId>/} inside a running user-owned stateful container and returns name /
 * sizeBytes / mtime for each. Wire contract: payload: {envId, imageRef, instanceId, templateId} result:
 * {files:[{name, sizeBytes, mtime}]} Uses a portable {@code sh -c} for-loop (busybox/debian) rather than GNU
 * {@code find -printf}, so Alpine images without GNU find still work.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulInstanceListFilesCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stateful-instance-list-files";

    private static final Duration LIST_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;
    private final OciCli cli;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(command.payload() == null ? "{}" : command.payload());
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: malformed payload: " + message(parse));
        }

        String instanceId = text(payload, "instanceId");
        String templateIdRaw = text(payload, "templateId");

        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: payload missing 'instanceId'");
        }
        if (templateIdRaw == null || templateIdRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: payload missing 'templateId'");
        }

        final String templateId;
        try {
            templateId = BespokeFilePaths.requireTemplateId(templateIdRaw);
        } catch (IllegalArgumentException bad) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: " + bad.getMessage());
        }

        return manager.byInstanceId(instanceId)
                .map(inst -> listFiles(inst, templateId))
                .orElseGet(() -> DaemonCommandOutcome.failed(
                        "stateful-instance-list-files: unknown instanceId " + instanceId));
    }

    private DaemonCommandOutcome listFiles(StatefulInstance inst, String templateId) {
        if (!inst.isRunning()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: instance is not running (status="
                            + inst.status() + ")");
        }
        if (BespokeFilePaths.isPlatformManaged(inst)) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: platform-managed instance refuses bespoke files");
        }
        if (!cli.available()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: container runtime unavailable");
        }

        String dir = BespokeFilePaths.containerDir(templateId);
        // Portable listing for busybox/debian: one TSV line per regular file.
        String script = "dir=" + BespokeFilePaths.shSingleQuote(dir)
                + "; if [ ! -d \"$dir\" ]; then exit 0; fi; "
                + "for f in \"$dir\"/*; do "
                + "[ -f \"$f\" ] || continue; "
                + "name=$(basename \"$f\"); "
                + "size=$(wc -c < \"$f\" | tr -d \" \"); "
                + "mtime=$(date -r \"$f\" +%s 2>/dev/null || stat -c %Y \"$f\" 2>/dev/null || echo 0); "
                + "printf \"%s\\t%s\\t%s\\n\" \"$name\" \"$size\" \"$mtime\"; "
                + "done";

        // The listing script bears double quotes, so it rides stdin (sh -s): a quote-bearing
        // sh -c argv element is shattered by the Win32 command-line round-trip on Windows
        // hosts (see ShExec).
        OciCli.Result r = cli.run(ShExec.stdinArgv(cli.cliPath(), inst.containerName()),
                ShExec.utf8(script), LIST_TIMEOUT);
        if (r.timedOut() || r.exitCode() != 0) {
            String err = r.firstErrLine();
            if (err == null || err.isBlank()) {
                err = r.timedOut() ? "timed out" : ("exit=" + r.exitCode());
            }
            log.warn("[T-29] stateful-instance-list-files failed templateId={}: {}",
                    templateId, err);
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files failed: " + err);
        }

        try {
            ObjectNode out = objectMapper.createObjectNode();
            ArrayNode files = out.putArray("files");
            parseListing(r.stdout(), files);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-list-files: could not encode result: " + message(e));
        }
    }

    /**
     * Parse {@code name\\tsizeBytes\\tmtime} lines. Skips blank lines and any name that fails
     * {@link BespokeFilePaths#requireBasename(String)}.
     */
    private void parseListing(String stdout, ArrayNode files) {
        if (stdout == null || stdout.isBlank()) {
            return;
        }
        for (String line : stdout.split("\n")) {
            if (line == null || line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\t", 3);
            if (parts.length < 3) {
                continue;
            }
            String rawName = parts[0];
            final String name;
            try {
                name = BespokeFilePaths.requireBasename(rawName);
            } catch (IllegalArgumentException skip) {
                continue;
            }
            long sizeBytes;
            long mtime;
            try {
                sizeBytes = Long.parseLong(parts[1].trim());
                // Truncate fractional epochs (e.g. GNU find %T@) if a future script emits them.
                String mtimeRaw = parts[2].trim();
                int dot = mtimeRaw.indexOf('.');
                if (dot >= 0) {
                    mtimeRaw = mtimeRaw.substring(0, dot);
                }
                mtime = Long.parseLong(mtimeRaw);
            } catch (NumberFormatException skip) {
                continue;
            }
            ObjectNode entry = files.addObject();
            entry.put("name", name);
            entry.put("sizeBytes", sizeBytes);
            entry.put("mtime", mtime);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
