package com.studioquack.sqs.sandbox.testrun;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.helperchan.HelperFailure;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter.RoutedResponse;
import com.studioquack.sqs.sandbox.oci.OciBuildPathValidator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON parse + 200-envelope helpers for {@link TestSandboxRunService}. Kept separate so the
 * orchestrator stays a single-responsibility flow under 500 lines.
 */
final class TestSandboxRunCodec {

    private static final int HARD_BUILD_CAP_SECONDS = 1200;

    private final ObjectMapper mapper;
    private final TestBuildProperties props;

    TestSandboxRunCodec(ObjectMapper mapper, TestBuildProperties props) {
        this.mapper = mapper;
        this.props = props;
    }

    JsonNode parseObject(byte[] body) {
        try {
            if (body == null || body.length == 0) {
                return null;
            }
            JsonNode n = mapper.readTree(body);
            return n != null && n.isObject() ? n : null;
        } catch (Exception e) {
            return null;
        }
    }

    List<StagedFile> parseFiles(JsonNode root) {
        JsonNode filesNode = root.get("files");
        if (filesNode == null || filesNode.isNull()) {
            throw new IllegalArgumentException("files is required");
        }
        List<StagedFile> files = new ArrayList<>();
        if (filesNode.isArray()) {
            if (filesNode.isEmpty()) {
                throw new IllegalArgumentException("files must be non-empty");
            }
            for (JsonNode item : filesNode) {
                if (item == null || !item.isObject()) {
                    throw new IllegalArgumentException("files entries must be {path, content} objects");
                }
                JsonNode pathNode = item.get("path");
                if (pathNode == null || !pathNode.isTextual() || pathNode.asText().isBlank()) {
                    throw new IllegalArgumentException("files[].path is required");
                }
                files.add(new StagedFile(pathNode.asText(), contentBytes(item.get("content"))));
            }
        } else if (filesNode.isObject()) {
            if (filesNode.isEmpty()) {
                throw new IllegalArgumentException("files must be non-empty");
            }
            var it = filesNode.fields();
            while (it.hasNext()) {
                var e = it.next();
                files.add(new StagedFile(e.getKey(), contentBytes(e.getValue())));
            }
        } else {
            throw new IllegalArgumentException("files must be an object or array");
        }
        boolean hasRoot = false;
        for (StagedFile f : files) {
            String n = OciBuildPathValidator.normalizeAndValidate(f.path());
            if ("Dockerfile".equals(n) || "Containerfile".equals(n)) {
                hasRoot = true;
            }
        }
        if (!hasRoot) {
            throw new IllegalArgumentException("files must include a root Dockerfile or Containerfile");
        }
        return files;
    }

    HelperJob childJob(String buildId, JsonNode root, HelperJob caller) {
        Map<String, Object> ec = mapOrEmpty(root.get("execution_context"));
        JsonNode request = root.get("request");
        if (request != null && request.isObject()) {
            overlay(ec, request, "requestId", "workflowExecutionId", "currentNodeId", "envId", "templateId");
        }
        if (caller != null && caller.executionContext() != null) {
            Map<String, Object> callerEc = caller.executionContext();
            ec.putIfAbsent("workflowExecutionId", callerEc.get("workflowExecutionId"));
            ec.putIfAbsent("currentNodeId", callerEc.get("currentNodeId"));
            ec.putIfAbsent("envId", callerEc.get("envId"));
        }
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        JsonNode inputData = root.get("input_data");
        if (inputData != null && inputData.isObject()) {
            var it = inputData.fields();
            while (it.hasNext()) {
                var e = it.next();
                try {
                    inputs.put(e.getKey(), mapper.writeValueAsBytes(e.getValue()));
                } catch (Exception ex) {
                    throw new IllegalArgumentException("input_data." + e.getKey() + " is not serializable");
                }
            }
        }
        return HelperJob.builder(buildId, "custom")
                .executionContext(ec)
                .inputs(inputs)
                .mockResponses(mapOrEmpty(root.get("mock_function_responses")))
                .build();
    }

    String buildRequestJson(JsonNode root, HelperJob child, String helperToken, boolean brokerReady) {
        Map<String, Object> req = new LinkedHashMap<>();
        Map<String, Object> ec = child.executionContext();
        req.put("requestId", str(ec.getOrDefault("requestId", child.id())));
        req.put("workflowExecutionId", ec.get("workflowExecutionId"));
        req.put("currentNodeId", ec.get("currentNodeId"));
        req.put("envId", ec.get("envId"));
        if (ec.get("templateId") != null && !str(ec.get("templateId")).isBlank()) {
            req.put("templateId", ec.get("templateId"));
        }
        req.put("executionContext", ec);
        req.put("inputVariables", mapOrEmpty(root.get("input_data")));
        if (helperToken != null) {
            req.put("helperToken", helperToken);
        }
        if (!brokerReady) {
            req.put("helperBrokerAbsent", "no helper broker in this container (image must ship python3)");
        }
        try {
            return mapper.writeValueAsString(req);
        } catch (Exception e) {
            return "{}";
        }
    }

    Map<String, Object> parseResponseBody(String body) {
        if (body == null || body.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            String trimmed = body.trim();
            if (!trimmed.startsWith("{")) {
                return new LinkedHashMap<>();
            }
            Map<String, Object> parsed = mapper.readValue(trimmed, new TypeReference<Map<String, Object>>() {});
            return parsed == null ? new LinkedHashMap<>() : parsed;
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    RoutedResponse envelope(boolean imageBuilt, String buildLog, boolean runAttempted,
                            boolean appListening, boolean brokerReady, Map<String, Object> output,
                            String stdout, String stderr, int exitCode,
                            List<Map<String, Object>> invocationLog,
                            List<Map<String, Object>> helperLogs,
                            Map<String, Object> failure, String error) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("image_built", imageBuilt);
        body.put("build_log", buildLog == null ? "" : buildLog);
        body.put("run_attempted", runAttempted);
        body.put("app_listening", appListening);
        body.put("broker_ready", brokerReady);
        body.put("output", output);
        body.put("stdout", stdout == null ? "" : stdout);
        body.put("stderr", stderr == null ? "" : stderr);
        body.put("exit_code", exitCode);
        body.put("invocation_log", invocationLog == null ? List.of() : invocationLog);
        body.put("helper_logs", helperLogs == null ? List.of() : helperLogs);
        body.put("failure", failure);
        body.put("error", error == null ? "" : error);
        return jsonStatus(200, body);
    }

    RoutedResponse json400(String message) {
        return jsonStatus(400, Map.of("error", message == null ? "" : message));
    }

    RoutedResponse jsonStatus(int status, Object body) {
        try {
            return RoutedResponse.json(status, mapper.writeValueAsBytes(body));
        } catch (Exception e) {
            return RoutedResponse.json(status, "{\"error\":\"internal\"}".getBytes(StandardCharsets.UTF_8));
        }
    }

    String cap(String s) {
        if (s == null) {
            return "";
        }
        int max = props == null ? 20_000 : Math.max(0, props.getMaxLogChars());
        if (max <= 0) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    int capRunTimeout(int requested) {
        int v = Math.max(1, requested);
        int capAt = props == null ? 600 : Math.max(1, props.getMaxRunTimeoutSeconds());
        return Math.min(v, capAt);
    }

    int capBuildWall(int requested) {
        int v = Math.max(1, requested);
        int propCap = props == null ? 600 : Math.max(1, props.getBuildWallClockSeconds());
        return Math.min(v, Math.min(HARD_BUILD_CAP_SECONDS, propCap));
    }

    int intOr(JsonNode root, String field, int fallback) {
        JsonNode n = root.get(field);
        if (n == null || n.isNull()) {
            return fallback;
        }
        if (n.isNumber()) {
            return n.asInt(fallback);
        }
        try {
            return Integer.parseInt(n.asText().trim());
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    static int parseRamMb(String runMemory) {
        if (runMemory == null || runMemory.isBlank()) {
            return 512;
        }
        String s = runMemory.trim();
        if (s.length() > 1 && (s.endsWith("m") || s.endsWith("M"))) {
            s = s.substring(0, s.length() - 1);
        }
        try {
            int n = Integer.parseInt(s.trim());
            return n > 0 ? n : 512;
        } catch (NumberFormatException e) {
            return 512;
        }
    }

    static Map<String, Object> failureMap(HelperJob child) {
        HelperFailure f = child == null ? null : child.failure();
        if (f == null || !f.hasCode()) {
            return null;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("errorCode", f.errorCode());
        m.put("message", f.message() == null ? "" : f.message());
        return m;
    }

    static List<Map<String, Object>> copyList(List<Map<String, Object>> src) {
        return src == null ? List.of() : new ArrayList<>(src);
    }

    static boolean looksLikeHttpCode(String stdout) {
        if (stdout == null) {
            return false;
        }
        String t = stdout.trim();
        return t.length() >= 3
                && Character.isDigit(t.charAt(0))
                && Character.isDigit(t.charAt(1))
                && Character.isDigit(t.charAt(2));
    }

    static String shortId(String id) {
        if (id == null || id.isBlank()) {
            return "";
        }
        return id.length() <= 12 ? id : id.substring(0, 12);
    }

    static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    static Object ecGet(HelperJob caller, String key) {
        if (caller == null || caller.executionContext() == null) {
            return null;
        }
        return caller.executionContext().get(key);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapOrEmpty(JsonNode n) {
        if (n == null || n.isNull() || !n.isObject()) {
            return new LinkedHashMap<>();
        }
        try {
            return mapper.convertValue(n, LinkedHashMap.class);
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static void overlay(Map<String, Object> ec, JsonNode request, String... keys) {
        for (String k : keys) {
            JsonNode v = request.get(k);
            if (v != null && !v.isNull()) {
                ec.put(k, v.isValueNode() ? (v.isNumber() ? v.numberValue() : v.asText()) : v);
            }
        }
    }

    private byte[] contentBytes(JsonNode content) {
        if (content == null || content.isNull()) {
            return new byte[0];
        }
        if (content.isTextual()) {
            return content.asText().getBytes(StandardCharsets.UTF_8);
        }
        try {
            return mapper.writeValueAsBytes(content);
        } catch (Exception e) {
            throw new IllegalArgumentException("files content is not serializable");
        }
    }

    record StagedFile(String path, byte[] content) {
    }
}
