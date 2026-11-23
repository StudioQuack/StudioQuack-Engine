package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts locally-buffered {@link SandboxLogEntity} rows into {@link SandboxLogWireRow}s for the
 * two non-live lanes. The backend persists these rows and spills the bytes to stored files, which
 * is what makes the Monitoring download work.
 */
@Slf4j
@Component
public class SandboxLogRowMapper {

    /** Whether a lane inlines file bytes or leaves the {@code sq-file:} handle in place. */
    public enum FileMode { HYDRATE, REF_ONLY }

    private final ObjectMapper mapper;
    private final SandboxLogFileHydrator hydrator;

    public SandboxLogRowMapper(ObjectMapper mapper, SandboxLogFileHydrator hydrator) {
        this.mapper = mapper;
        this.hydrator = hydrator;
    }

    /** Maps an ordered list of entities to wire rows, preserving order. Never throws. */
    public List<SandboxLogWireRow> toWireRows(List<SandboxLogEntity> entities, FileMode fileMode) {
        List<SandboxLogWireRow> rows = new ArrayList<>();
        if (entities == null) {
            return rows;
        }
        for (SandboxLogEntity e : entities) {
            if (e != null) {
                rows.add(toWireRow(e, fileMode));
            }
        }
        return rows;
    }

    /** Maps one entity to a wire row (JSON fields parsed; file handles per {@code fileMode}). Never throws. */
    public SandboxLogWireRow toWireRow(SandboxLogEntity e, FileMode fileMode) {
        boolean hydrate = fileMode == FileMode.HYDRATE;
        return new SandboxLogWireRow(
                e.getNodeId(),
                e.getNodeName(),
                e.getStatus(),
                e.getExitCode(),
                e.getDurationMs(),
                e.getLanguage(),
                e.getContainerId(),
                e.getStdout(),
                e.getStderr(),
                e.getErrorMessage(),
                files(parse(e.getInputJson()), hydrate),
                files(parse(e.getOutputJson()), hydrate),
                files(parse(e.getAppliedInputsJson()), hydrate),
                parseUnknownInputParameters(e.getUnknownInputParametersJson()),
                e.getFailureKind(),
                e.getPostureNote(),
                e.getCdate() == null ? null : e.getCdate().toString(),
                // Per-attempt identity: the local row's own id, so a flushed/pulled row is
                // recognisably the SAME delivery the live STREAM post carried (and vice versa).
                e.getId(),
                Boolean.TRUE.equals(e.getRetry()) ? Boolean.TRUE : null);
    }

    /** {@link FileMode#HYDRATE} inlines bytes; {@link FileMode#REF_ONLY} returns the parsed tree as stored. */
    private Object files(Object parsed, boolean hydrate) {
        return hydrate ? hydrator.hydrate(parsed) : parsed;
    }

    /** Parses a stored JSON string back to an object; null / unparseable degrades to null. */
    private Object parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return mapper.readValue(json, Object.class);
        } catch (Exception e) {
            log.debug("sandbox-log[row-mapper]: stored IO JSON not parseable ({}); passing null", e.toString());
            return null;
        }
    }

    /** Parses stored unknown-input-parameters JSON back to a string list; null on empty / failure. */
    @SuppressWarnings("unchecked")
    private java.util.List<String> parseUnknownInputParameters(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return mapper.readValue(json, java.util.List.class);
        } catch (Exception e) {
            log.debug("sandbox-log[row-mapper]: unknownInputParameters JSON not parseable ({}); passing null",
                    e.toString());
            return null;
        }
    }
}
