package com.studioquack.sqs.docling;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DoclingClient} wiring tests: loopback URL parity with {@link DoclingContainer}, the
 * budgets applied to the HTTP factory (asserted without sleeping), the multipart contract — and
 * the async submit → poll → result flow against a loopback stand-in for docling-serve.
 */
class DoclingClientTest {

    private HttpServer fakeDocling;

    @AfterEach
    void tearDown() {
        if (fakeDocling != null) {
            fakeDocling.stop(0);
        }
    }

    @Test
    @DisplayName("a blank/absent base URL falls back to the loopback side-car, not a DNS name")
    void blankBaseUrlFallsBackToLoopback() {
        assertEquals(DoclingClient.DEFAULT_BASE_URL + DoclingClient.CONVERT_ASYNC_PATH,
                new DoclingClient("  ",
                        DoclingClient.DEFAULT_CONVERT_TIMEOUT_SECONDS,
                        DoclingClient.DEFAULT_CONNECT_TIMEOUT_SECONDS).submitUrl());
        assertTrue(DoclingClient.DEFAULT_BASE_URL.contains("127.0.0.1"),
                "the side-car publishes on loopback only — a resolvable host name is a bug");
    }

    @Test
    @DisplayName("the client and the container agree on the same default side-car URL")
    void clientAndContainerDefaultsMatch() {
        assertEquals(DoclingClient.DEFAULT_BASE_URL, valueDefaultOf(DoclingClient.class, "sqs.docling.url"),
                "DoclingClient's @Value default must be the shared constant");
        assertEquals(DoclingClient.DEFAULT_BASE_URL, valueDefaultOf(DoclingContainer.class, "sqs.docling.url"),
                "DoclingContainer probes the URL DoclingClient posts to — one default, not two");
    }

    @Test
    @DisplayName("default convert budget is 900s; each async-API call gets the short read timeout")
    void defaultConvertBudgetAndReadTimeout() {
        DoclingClient client = new DoclingClient(DoclingClient.DEFAULT_BASE_URL,
                DoclingClient.DEFAULT_CONVERT_TIMEOUT_SECONDS,
                DoclingClient.DEFAULT_CONNECT_TIMEOUT_SECONDS);
        assertEquals(900, client.convertTimeoutSeconds());
        assertEquals(DoclingClient.REQUEST_READ_TIMEOUT_SECONDS, client.readTimeoutSeconds(),
                "submit/poll/result all answer in seconds — the read timeout is per request now");
        assertEquals(5, client.connectTimeoutSeconds());
    }

    @Test
    @DisplayName("a custom convert budget is what the poll loop applies")
    void customConvertBudgetApplied() {
        DoclingClient client = new DoclingClient(DoclingClient.DEFAULT_BASE_URL, 600, 3);
        assertEquals(600, client.convertTimeoutSeconds());
        assertEquals(DoclingClient.REQUEST_READ_TIMEOUT_SECONDS, client.readTimeoutSeconds());
        assertEquals(3, client.connectTimeoutSeconds());
    }

    @Test
    @DisplayName("convert-timeout-seconds @Value default is the poll-loop budget (900s)")
    void convertTimeoutValueDefaultIs900() {
        assertEquals("900", valueDefaultOf(DoclingClient.class, "sqs.docling.convert-timeout-seconds"));
    }

    @Test
    @DisplayName("DoclingConvertSettings @Value defaults match interactiveDefaults()")
    void convertSettingsValueDefaultsMatchInteractiveFactory() {
        DoclingConvertSettings d = DoclingConvertSettings.interactiveDefaults();
        assertEquals(Boolean.toString(d.doOcr), valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.do-ocr"));
        assertEquals(Boolean.toString(d.forceOcr), valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.force-ocr"));
        assertEquals(d.ocrEngine, valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.ocr-engine"));
        assertEquals(Boolean.toString(d.doTableStructure),
                valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.do-table-structure"));
        assertEquals(d.tableMode, valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.table-mode"));
        assertEquals(Double.toString(d.imagesScale),
                valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.images-scale"));
        assertEquals(Boolean.toString(d.includeImages),
                valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.include-images"));
        assertEquals(d.imageExportMode,
                valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.image-export-mode"));
        assertEquals(d.toFormats, valueDefaultOf(DoclingConvertSettings.class, "sqs.docling.to-formats"));
        assertEquals("4", valueDefaultOf(DoclingContainer.class, "sqs.docling.num-threads"));
        // Blank = auto: DoclingImageResolver picks cuda for the GPU image, cpu otherwise.
        assertEquals("", valueDefaultOf(DoclingContainer.class, "sqs.docling.device"));
    }

    @Test
    @DisplayName("convert body posts interactive ConvertDocumentsOptions as multipart text fields")
    void buildConvertBodyIncludesInteractiveOptionFields() {
        DoclingClient client = new DoclingClient(DoclingClient.DEFAULT_BASE_URL,
                DoclingClient.DEFAULT_CONVERT_TIMEOUT_SECONDS,
                DoclingClient.DEFAULT_CONNECT_TIMEOUT_SECONDS);
        MultiValueMap<String, Object> body = client.buildConvertBody(new byte[]{1}, "scan.gif", "image/gif");
        assertEquals("true", body.getFirst("do_ocr"));
        assertEquals("false", body.getFirst("force_ocr"));
        assertEquals("tesseract", body.getFirst("ocr_engine"));
        assertEquals("tesseract", body.getFirst("ocr_preset"));
        assertEquals("false", body.getFirst("do_table_structure"));
        assertEquals("fast", body.getFirst("table_mode"));
        assertEquals("1.0", body.getFirst("images_scale"));
        assertEquals("false", body.getFirst("include_images"));
        assertEquals("placeholder", body.getFirst("image_export_mode"));
        assertEquals("md", body.getFirst("to_formats"));
        assertEquals("false", body.getFirst("do_code_enrichment"));
        assertEquals("false", body.getFirst("do_formula_enrichment"));
        assertEquals("false", body.getFirst("do_picture_classification"));
        assertEquals("false", body.getFirst("do_picture_description"));
        assertEquals("false", body.getFirst("do_chart_extraction"));
        assertTrue(body.containsKey("files"), "files part must still be present");
    }

    @Test
    @DisplayName("convert submits async, polls through started→success, and returns the result body")
    void convertRunsTheAsyncFlow() throws Exception {
        AtomicReference<String> submittedBody = new AtomicReference<>();
        AtomicInteger polls = new AtomicInteger();
        startFakeDocling(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                submittedBody.set(new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8));
                respond(exchange, 200, "{\"task_id\":\"task-1\",\"task_status\":\"pending\"}");
            } else if ((DoclingClient.STATUS_POLL_PATH + "task-1").equals(path)) {
                // One "started" answer first so the poll loop itself is exercised.
                respond(exchange, 200, polls.incrementAndGet() == 1
                        ? "{\"task_id\":\"task-1\",\"task_status\":\"started\"}"
                        : "{\"task_id\":\"task-1\",\"task_status\":\"success\"}");
            } else if ((DoclingClient.RESULT_PATH + "task-1").equals(path)) {
                respond(exchange, 200, "{\"document\":{\"md_content\":\"# converted\"}}");
            } else {
                respond(exchange, 404, "{\"detail\":\"unexpected path\"}");
            }
        });
        DoclingClient client = clientForFake();

        String body = client.convert("pdf-bytes".getBytes(StandardCharsets.UTF_8),
                "doc.pdf", "application/pdf");

        assertEquals("{\"document\":{\"md_content\":\"# converted\"}}", body,
                "the result endpoint's envelope is returned verbatim (the delegate unwraps it)");
        assertTrue(submittedBody.get() != null && submittedBody.get().contains("doc.pdf"),
                "the multipart body carries the document filename to the async endpoint");
        assertTrue(polls.get() >= 2, "a started task is polled again: " + polls.get());
    }

    @Test
    @DisplayName("a failed task surfaces the server's own error_message, not a 404 riddle")
    void failedTaskCarriesTheServerError() throws Exception {
        startFakeDocling(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"task-9\",\"task_status\":\"pending\"}");
            } else if ((DoclingClient.STATUS_POLL_PATH + "task-9").equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"task-9\",\"task_status\":\"failure\","
                        + "\"error_message\":\"whisper is not installed\"}");
            } else {
                respond(exchange, 404, "{\"detail\":\"unexpected path\"}");
            }
        });
        DoclingClient client = clientForFake();

        DoclingConversionException ex = assertThrows(DoclingConversionException.class,
                () -> client.convert("audio".getBytes(StandardCharsets.UTF_8),
                        "voice.webm", "audio/webm"));

        assertTrue(ex.getMessage().contains("whisper is not installed"),
                "the server's sanitised error reaches the caller: " + ex.getMessage());
    }

    @Test
    @DisplayName("a skipped document (input-gate rejection) fails instead of sealing the error JSON")
    void skippedDocumentFailsWithTheServersReason() throws Exception {
        startFakeDocling(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"task-7\",\"task_status\":\"pending\"}");
            } else if ((DoclingClient.STATUS_POLL_PATH + "task-7").equals(path)) {
                // The TASK succeeds — only the document inside was rejected at the input gate.
                respond(exchange, 200, "{\"task_id\":\"task-7\",\"task_status\":\"success\"}");
            } else if ((DoclingClient.RESULT_PATH + "task-7").equals(path)) {
                respond(exchange, 200, "{\"document\":{\"filename\":\"voice.webm\","
                        + "\"md_content\":null},\"status\":\"skipped\",\"errors\":[{"
                        + "\"component_type\":\"user_input\",\"module_name\":\"\","
                        + "\"error_message\":\"File format not allowed: voice.webm\"}]}");
            } else {
                respond(exchange, 404, "{\"detail\":\"unexpected path\"}");
            }
        });
        DoclingClient client = clientForFake();

        DoclingConversionException ex = assertThrows(DoclingConversionException.class,
                () -> client.convert("audio".getBytes(StandardCharsets.UTF_8),
                        "voice.webm", "audio/webm"));

        assertTrue(ex.getMessage().contains("File format not allowed: voice.webm"),
                "the document-level reason reaches the caller: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("skipped"),
                "the document status is named: " + ex.getMessage());
    }

    @Test
    @DisplayName("a not-yet-stored result (404) is retried, then served")
    void resultNotFoundIsRetried() throws Exception {
        AtomicInteger resultFetches = new AtomicInteger();
        startFakeDocling(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"task-5\",\"task_status\":\"pending\"}");
            } else if ((DoclingClient.STATUS_POLL_PATH + "task-5").equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"task-5\",\"task_status\":\"success\"}");
            } else if ((DoclingClient.RESULT_PATH + "task-5").equals(path)) {
                if (resultFetches.incrementAndGet() == 1) {
                    respond(exchange, 404,
                            "{\"detail\":\"Task result not found. Please wait for a completion status.\"}");
                } else {
                    respond(exchange, 200, "{\"document\":{\"md_content\":\"ok\"}}");
                }
            } else {
                respond(exchange, 404, "{\"detail\":\"unexpected path\"}");
            }
        });
        DoclingClient client = clientForFake();

        String body = client.convert("x".getBytes(StandardCharsets.UTF_8), "a.pdf", "application/pdf");

        assertEquals("{\"document\":{\"md_content\":\"ok\"}}", body);
        assertEquals(2, resultFetches.get(), "one 404 then one success");
    }

    @Test
    @DisplayName("a vanished task (404 on the poll) fails with a named error")
    void vanishedTaskFailsNamed() throws Exception {
        startFakeDocling(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (DoclingClient.CONVERT_ASYNC_PATH.equals(path)) {
                respond(exchange, 200, "{\"task_id\":\"gone\",\"task_status\":\"pending\"}");
            } else {
                respond(exchange, 404, "{\"detail\":\"Task not found.\"}");
            }
        });
        DoclingClient client = clientForFake();

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> client.convert("x".getBytes(StandardCharsets.UTF_8), "a.pdf", "application/pdf"));

        assertTrue(ex.getMessage().contains("no longer knows the conversion task"),
                "an evicted task is named as such: " + ex.getMessage());
    }

    /** A client pointed at the loopback fake, with production-default budgets. */
    private DoclingClient clientForFake() {
        return new DoclingClient("http://127.0.0.1:" + fakeDocling.getAddress().getPort(),
                DoclingClient.DEFAULT_CONVERT_TIMEOUT_SECONDS,
                DoclingClient.DEFAULT_CONNECT_TIMEOUT_SECONDS);
    }

    private void startFakeDocling(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        fakeDocling = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeDocling.createContext("/", handler);
        fakeDocling.start();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    /** The fallback baked into a bean constructor {@code @Value} for the given property prefix. */
    private static String valueDefaultOf(Class<?> beanType, String property) {
        String prefix = "${" + property + ":";
        for (Constructor<?> ctor : beanType.getConstructors()) {
            for (Parameter p : ctor.getParameters()) {
                Value v = p.getAnnotation(Value.class);
                if (v != null && v.value().startsWith(prefix)) {
                    return v.value().substring(prefix.length(), v.value().length() - 1);
                }
            }
        }
        throw new AssertionError(beanType.getSimpleName() + " has no " + property + " parameter");
    }
}
