package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager.LoadResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared upload-direct-to-daemon OCI image ingress logic, extracted from {@code
 * OciImageUploadController} so it can be reused verbatim by BOTH ingress fronts without a second
 * execution path.
 */
@Slf4j
@Component
public class OciImageBlobIngestService {

    private final StatefulRuntimeProperties props;
    private final OciImageBlobStore blobStore;
    private final StatefulInstanceManager manager;
    private final OciImageDigestReporter digestReporter;

    public OciImageBlobIngestService(StatefulRuntimeProperties props,
                                     OciImageBlobStore blobStore,
                                     StatefulInstanceManager manager,
                                     OciImageDigestReporter digestReporter) {
        this.props = props;
        this.blobStore = blobStore;
        this.manager = manager;
        this.digestReporter = digestReporter;
    }

    /**
     * A transport-neutral ingress outcome: the HTTP status to reply with + the JSON body map. Both
     * the servlet controller and the JDK-HttpServer overlay listener render this the same way.
     */
    public record Result(int httpStatus, Map<String, Object> body) {
    }

    /**
     * Authenticates, stores the tarball, {@code podman load}s it, and reports the digest — identical
     * to the original controller flow. The caller owns nothing after the call: the {@code body} stream
     * is fully consumed and closed here.
     *
     * @param ref The (env-scoped) image ref path variable.
     */
    public Result ingest(String ref, String envId, String displayName,
                         InputStream body, String presentedBearer) {
        // 0) Fail-closed: no configured upload token → the endpoint is disabled.
        String configured = props.getUploadToken();
        if (StringUtils.isBlank(configured)) {
            log.warn("[SQ-6] oci-image upload rejected reason=ingress_disabled (no sqs.sandbox.oci.upload-token)");
            return error(503, "Error_IngressDisabled", "OCI image upload ingress is not enabled on this daemon.");
        }

        // 1) Authenticate LOCALLY — constant-time bearer compare (401 on missing / no match).
        if (StringUtils.isBlank(presentedBearer)) {
            return error(401, "Error_MissingToken", "Authorization bearer token is required.");
        }
        if (!constantTimeEquals(presentedBearer, configured.trim())) {
            log.warn("[SQ-6] oci-image upload rejected reason=invalid_token ref={}", ref);
            return error(401, "Error_InvalidToken", "Invalid upload token.");
        }

        // 2) Stream the tarball to the blob store (512 MiB ceiling), then podman load.
        OciImageBlobStore.StoredBlob stored;
        try (InputStream in = body) {
            stored = blobStore.store(ref, in);
        } catch (OciImageBlobStore.TooLarge e) {
            log.warn("[SQ-6] oci-image upload rejected reason=too_large ref={}: {}", ref, e.getMessage());
            return error(413, "Error_RequestBodyTooLarge", e.getMessage());
        } catch (Exception e) {
            log.warn("[SQ-6] oci-image upload stream failed ref={}: {}", ref, e.toString());
            return error(500, "Error_UploadFailed", "Failed to store the uploaded image blob.");
        }
        if (stored.bytes() <= 0) {
            return error(400, "Error_EmptyUpload", "The uploaded image blob was empty.");
        }

        LoadResult load = manager.loadImage(envId, ref, displayName);

        // 3) Report the digest back to the control plane registry (best-effort; command-result is
        // canonical).
        boolean reported = false;
        if (StatefulStatus.LOADED.equals(load.status())) {
            reported = digestReporter.report(envId, ref, load.digest(), load.status());
        }

        Map<String, Object> body2 = new LinkedHashMap<>();
        body2.put("ref", load.ref());
        body2.put("digest", load.digest());
        body2.put("status", load.status());
        body2.put("bytes", stored.bytes());
        body2.put("digestReported", reported);
        int code = StatefulStatus.LOADED.equals(load.status()) ? 200 : 502;
        log.info("[SQ-6] oci-image upload ref={} bytes={} status={} digestReported={}",
                ref, stored.bytes(), load.status(), reported);
        return new Result(code, body2);
    }

    /** Constant-time equality so the token compare does not leak length/prefix via timing. */
    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static Result error(int status, String code, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", code);
        body.put("message", message == null ? "" : message);
        return new Result(status, body);
    }
}
