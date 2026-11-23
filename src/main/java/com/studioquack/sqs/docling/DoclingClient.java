package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * The sync {@code /v1/convert/file} endpoint is. The poll payload carries the server's sanitised
 * {@code error_message}, surfaced here as {@link DoclingConversionException}.
 */
@Slf4j
@Component
public class DoclingClient {

    /** Async submission endpoint appended to the configured base URL. */
    public static final String CONVERT_ASYNC_PATH = "/v1/convert/file/async";
    /** Task-status poll endpoint prefix; the task id is appended. */
    public static final String STATUS_POLL_PATH = "/v1/status/poll/";
    /** Task-result endpoint prefix; the task id is appended. */
    public static final String RESULT_PATH = "/v1/result/";
    /** Multipart form field name Docling expects for the uploaded document(s). */
    private static final String FILE_PART = "files";

    /**
     * WHY: a multi-megabyte scanned image / GIF through RapidOCR on CPU routinely exceeds 2 minutes
     * (real run: 5MB GIF still converting at 121s).
     */
    static final int DEFAULT_CONVERT_TIMEOUT_SECONDS = 900;

    /**
     * Per-request HTTP read timeout seconds. Every async-API call answers in seconds (the submit
     * only enqueues; polls and the result fetch are lookups), so a minute is generous headroom for
     * a large multipart upload over the overlay — and a wedged server surfaces fast.
     */
    static final int REQUEST_READ_TIMEOUT_SECONDS = 60;

    /**
     * Default connect timeout seconds.
     * WHY: loopback side-car either answers immediately or is broken — keep this short.
     */
    static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 5;

    /** Delay between status polls. Mirrors docling-serve's own sync_poll_interval. */
    static final int POLL_INTERVAL_SECONDS = 2;

    /** Result-fetch retries against a not-yet-stored result (defensive; see {@link #fetchResult}). */
    private static final int RESULT_FETCH_ATTEMPTS = 3;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String baseUrl;
    private final String submitUrl;
    private final RestTemplate restTemplate;
    private final int convertTimeoutSeconds;
    private final int connectTimeoutSeconds;
    private final DoclingConvertSettings convertSettings;

    /** Loopback default, identical to {@link DoclingContainer}'s — the side-car publishes there only. */
    static final String DEFAULT_BASE_URL = "http://127.0.0.1:5001";

    /**
     * @param baseUrl {@code sqs.docling.url} (default {@link #DEFAULT_BASE_URL}); tests point this at
     * an in-process mock server.
     * @param convertTimeoutSeconds {@code sqs.docling.convert-timeout-seconds} — overall convert
     * budget (default {@link #DEFAULT_CONVERT_TIMEOUT_SECONDS}), enforced by the poll loop.
     * @param connectTimeoutSeconds {@code sqs.docling.connect-timeout-seconds} — TCP connect timeout
     * (default {@link #DEFAULT_CONNECT_TIMEOUT_SECONDS}).
     * @param convertSettings
     */
    @Autowired
    public DoclingClient(
            @Value("${sqs.docling.url:http://127.0.0.1:5001}") String baseUrl,
            @Value("${sqs.docling.convert-timeout-seconds:900}") int convertTimeoutSeconds,
            @Value("${sqs.docling.connect-timeout-seconds:5}") int connectTimeoutSeconds,
            DoclingConvertSettings convertSettings) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        this.submitUrl = this.baseUrl + CONVERT_ASYNC_PATH;
        this.convertTimeoutSeconds = Math.max(1, convertTimeoutSeconds);
        this.connectTimeoutSeconds = Math.max(1, connectTimeoutSeconds);
        this.convertSettings = convertSettings == null
                ? DoclingConvertSettings.interactiveDefaults() : convertSettings;
        this.restTemplate = buildRestTemplate(this.connectTimeoutSeconds);
        // Names/values only — never document bytes or converted text.
        log.info("docling: HTTP client enabled — submitUrl={}, convertBudgetSeconds={}, "
                        + "readTimeoutSeconds={} (per request), connectTimeoutSeconds={}, options=[{}]",
                this.submitUrl, this.convertTimeoutSeconds, REQUEST_READ_TIMEOUT_SECONDS,
                this.connectTimeoutSeconds, this.convertSettings.summarize());
    }

    /**
     * Test-friendly ctor — uses {@link DoclingConvertSettings#interactiveDefaults()}.
     * Package-visible so Spring sees only the {@code @Autowired} constructor.
     */
    DoclingClient(String baseUrl, int convertTimeoutSeconds, int connectTimeoutSeconds) {
        this(baseUrl, convertTimeoutSeconds, connectTimeoutSeconds,
                DoclingConvertSettings.interactiveDefaults());
    }

    /**
     * @param bytes
     * @param filename
     * @param mimeType
     * @return The result endpoint's body (the {@code ConvertDocumentResponse} envelope — the same
     * shape the sync endpoint returned), or {@code null} on an empty response
     * @throws DoclingConversionException When the server ran the task and reported FAILURE — the
     * message is the server's own sanitised error
     */
    public String convert(byte[] bytes, String filename, String mimeType) {
        return convert(bytes, filename, mimeType, null);
    }

    /**
     * Converts with an explicit {@code to_formats} override (the node's {@code output_format}).
     * Blank/null falls back to the configured default ({@code sqs.docling.to-formats}, {@code md}).
     */
    public String convert(byte[] bytes, String filename, String mimeType, String toFormats) {
        MultiValueMap<String, Object> body = buildConvertBody(bytes, filename, mimeType, toFormats);
        String taskId = submit(body);
        awaitCompletion(taskId);
        return fetchResult(taskId);
    }

    /**
     * Enqueues the conversion and returns the task id. The multipart contract is identical to the
     * sync endpoint's; the answer is a {@code TaskStatusResponse} whose {@code task_id} is the
     * handle for the poll/result calls.
     */
    private String submit(MultiValueMap<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(submitUrl, request, String.class);
        String taskId = parseTextField(response.getBody(), "task_id");
        if (StringUtils.isBlank(taskId)) {
            throw new IllegalStateException("docling-serve accepted the conversion but returned no "
                    + "task id — cannot track it (is the serving engine a compatible docling-serve?)");
        }
        log.debug("docling: conversion submitted taskId={}", taskId);
        return taskId;
    }

    /**
     * Polls {@code /v1/status/poll/{taskId}} until the task succeeds, fails, or the convert budget
     * runs out. A FAILURE poll payload carries the server's sanitised {@code error_message} — the
     * whole point of the async API — surfaced verbatim in a {@link DoclingConversionException}.
     */
    private void awaitCompletion(String taskId) {
        String pollUrl = baseUrl + STATUS_POLL_PATH + taskId;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(convertTimeoutSeconds);
        while (true) {
            JsonNode status = getJson(pollUrl, "status poll");
            if (status == null) {
                throw new IllegalStateException("docling-serve returned an empty status for task "
                        + taskId + " — cannot tell whether the conversion is still running");
            }
            String state = status.path("task_status").asText("").toLowerCase(Locale.ROOT);
            switch (state) {
                case "success" -> {
                    return;
                }
                case "failure" -> throw new DoclingConversionException(failureMessageFrom(status));
                default -> {
                    // pending/started (or an unknown future state): keep waiting within the budget.
                }
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("docling conversion task " + taskId + " did not finish "
                        + "within the convert budget of " + convertTimeoutSeconds
                        + "s (sqs.docling.convert-timeout-seconds) — the side-car is probably still "
                        + "busy; raise that property if documents need longer");
            }
            sleep(POLL_INTERVAL_SECONDS);
        }
    }

    /**
     * Fetches {@code /v1/result/{taskId}}. The local orchestrator stores the result BEFORE flipping
     * the task to SUCCESS, so a 404 here should be impossible — but other orchestrators make no such
     * promise, so a "Task result not found" 404 gets a couple of short retries before failing.
     */
    private String fetchResult(String taskId) {
        String resultUrl = baseUrl + RESULT_PATH + taskId;
        for (int attempt = 1; attempt <= RESULT_FETCH_ATTEMPTS; attempt++) {
            try {
                ResponseEntity<String> response = restTemplate.getForEntity(resultUrl, String.class);
                return requireDocumentSucceeded(response.getBody());
            } catch (HttpClientErrorException.NotFound notFound) {
                if (attempt == RESULT_FETCH_ATTEMPTS) {
                    throw new IllegalStateException("docling-serve reported task " + taskId
                            + " successful but its result is not retrievable (404 on " + RESULT_PATH
                            + "{taskId}) — the serving engine may have evicted it", notFound);
                }
                log.debug("docling: result for task {} not stored yet (attempt {}/{})",
                        taskId, attempt, RESULT_FETCH_ATTEMPTS);
                sleep(1);
            }
        }
        throw new IllegalStateException("unreachable"); // the loop always returns or throws
    }

    /**
     * A task can poll SUCCESS while its DOCUMENT was rejected at the input gate: the result envelope
     * carries the per-document {@code status} ({@code skipped} for "File format not allowed",
     * {@code failure} for a pipeline error) with the reason. Without this check the delegate seals the
     * error JSON as the converted output and the run COMPLETES with garbage.
     */
    private static String requireDocumentSucceeded(String body) {
        if (StringUtils.isBlank(body)) {
            return body;
        }
        JsonNode envelope;
        try {
            envelope = JSON.readTree(body);
        } catch (Exception notJson) {
            return body;
        }
        JsonNode statusNode = envelope.path("status");
        if (!statusNode.isTextual()) {
            return body;
        }
        String docStatus = statusNode.asText("").toLowerCase(Locale.ROOT);
        if (!"skipped".equals(docStatus) && !"failure".equals(docStatus)) {
            return body;
        }
        StringBuilder reasons = new StringBuilder();
        for (JsonNode error : envelope.path("errors")) {
            String msg = error.path("error_message").asText(null);
            if (StringUtils.isNotBlank(msg)) {
                if (reasons.length() > 0) {
                    reasons.append("; ");
                }
                reasons.append(msg);
            }
        }
        throw new DoclingConversionException("the document was not converted (status=" + docStatus
                + "): " + (reasons.length() == 0 ? "no error detail in the result envelope" : reasons));
    }

    /** One GET expecting a JSON body; {@code null} when the body is empty. 404 → task gone. */
    private JsonNode getJson(String url, String what) {
        ResponseEntity<String> response;
        try {
            response = restTemplate.getForEntity(url, String.class);
        } catch (HttpClientErrorException.NotFound notFound) {
            throw new IllegalStateException("docling-serve no longer knows the conversion task (404 on "
                    + what + ") — it was evicted or the side-car restarted mid-conversion", notFound);
        }
        String body = response.getBody();
        if (StringUtils.isBlank(body)) {
            return null;
        }
        try {
            return JSON.readTree(body);
        } catch (Exception notJson) {
            throw new IllegalStateException("docling-serve answered the " + what
                    + " with a non-JSON body — is something else serving this port?", notJson);
        }
    }

    /**
     * The server's own failure line from a FAILURE poll payload: {@code error_message} first
     * (docling-serve fills it with its sanitised public task error), then the {@code failure}
     * object, then a named fallback. Never blank.
     */
    private static String failureMessageFrom(JsonNode status) {
        String message = status.path("error_message").asText(null);
        if (StringUtils.isNotBlank(message)) {
            return message;
        }
        JsonNode failure = status.path("failure");
        if (failure.isObject() && StringUtils.isNotBlank(failure.path("message").asText(null))) {
            return failure.path("message").asText();
        }
        if (failure.isTextual() && StringUtils.isNotBlank(failure.asText())) {
            return failure.asText();
        }
        return "the conversion task failed on the serving engine without an error message — "
                + "check that engine's Docling container logs for 'failed to process job'";
    }

    /** Reads a top-level text field from a JSON body; {@code null} when absent/unparseable. */
    private static String parseTextField(String body, String field) {
        if (StringUtils.isBlank(body)) {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(body).path(field);
            return node.isTextual() ? node.asText() : null;
        } catch (Exception notJson) {
            return null;
        }
    }

    private static void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the Docling conversion",
                    interrupted);
        }
    }

    /**
     * Builds the multipart body for {@code /v1/convert/file/async} (file part +
     * ConvertDocumentsOptions). Package-visible so tests can assert form fields without posting.
     */
    MultiValueMap<String, Object> buildConvertBody(byte[] bytes, String filename, String mimeType) {
        return buildConvertBody(bytes, filename, mimeType, null);
    }

    /**
     * Builds the multipart body with an explicit {@code to_formats} override. The node-level
     * {@code output_format} (markdown/text) maps here 1:1; blank keeps the configured default.
     */
    MultiValueMap<String, Object> buildConvertBody(byte[] bytes, String filename, String mimeType,
                                                   String toFormatsOverride) {
        String safeFilename = StringUtils.isBlank(filename) ? "document" : filename;

        HttpHeaders fileHeaders = new HttpHeaders();
        fileHeaders.setContentType(StringUtils.isBlank(mimeType)
                ? MediaType.APPLICATION_OCTET_STREAM : MediaType.parseMediaType(mimeType));
        fileHeaders.setContentDispositionFormData(FILE_PART, safeFilename);

        Resource fileResource = new ByteArrayResource(bytes == null ? new byte[0] : bytes) {
            @Override
            public String getFilename() {
                return safeFilename; // multipart requires a filename on the file part
            }
        };
        HttpEntity<Resource> filePart = new HttpEntity<>(fileResource, fileHeaders);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add(FILE_PART, filePart);
        // Text parts match docling-serve curl examples: -F do_ocr=true …
        DoclingConvertSettings s = convertSettings;
        body.add("do_ocr", Boolean.toString(s.doOcr));
        body.add("force_ocr", Boolean.toString(s.forceOcr));
        // ocr_engine still in usage.md curl examples; ocr_preset is the newer preferred field.
        body.add("ocr_engine", s.ocrEngine);
        body.add("ocr_preset", s.ocrEngine);
        body.add("do_table_structure", Boolean.toString(s.doTableStructure));
        body.add("table_mode", s.tableMode);
        body.add("images_scale", Double.toString(s.imagesScale));
        body.add("include_images", Boolean.toString(s.includeImages));
        body.add("image_export_mode", s.imageExportMode);
        body.add("to_formats", StringUtils.isBlank(toFormatsOverride) ? s.toFormats : toFormatsOverride.trim());
        body.add("do_code_enrichment", Boolean.toString(s.doCodeEnrichment));
        body.add("do_formula_enrichment", Boolean.toString(s.doFormulaEnrichment));
        body.add("do_picture_classification", Boolean.toString(s.doPictureClassification));
        body.add("do_picture_description", Boolean.toString(s.doPictureDescription));
        body.add("do_chart_extraction", Boolean.toString(s.doChartExtraction));
        return body;
    }

    /** The resolved async submission endpoint (diagnostics / tests). */
    public String submitUrl() {
        return submitUrl;
    }

    /** Overall convert budget seconds ({@code sqs.docling.convert-timeout-seconds}). */
    int convertTimeoutSeconds() {
        return convertTimeoutSeconds;
    }

    /** Applied per-request HTTP read timeout in seconds ({@link #REQUEST_READ_TIMEOUT_SECONDS}). */
    int readTimeoutSeconds() {
        return REQUEST_READ_TIMEOUT_SECONDS;
    }

    /** Applied connect timeout in seconds. */
    int connectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    private static RestTemplate buildRestTemplate(int connectTimeoutSeconds) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(connectTimeoutSeconds));
        factory.setReadTimeout(Duration.ofSeconds(REQUEST_READ_TIMEOUT_SECONDS));
        return new RestTemplate(factory);
    }

    private static String normalizeBaseUrl(String baseUrl) {
        String url = StringUtils.isBlank(baseUrl) ? DEFAULT_BASE_URL : baseUrl.trim();
        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }
}
