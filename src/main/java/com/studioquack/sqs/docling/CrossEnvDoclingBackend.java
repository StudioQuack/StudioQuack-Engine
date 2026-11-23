package com.studioquack.sqs.docling;

import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;

import java.util.Optional;

/**
 * Cross-environment Docling backend: converts the document on a SAME-OWNER sibling engine's
 * Docling side-car, reached daemon→daemon over the WireGuard overlay — the reverse channel, never
 * the control plane. Route: the target engine publishes its Docling listener as a reverse port on
 * its overlay address ({@link DoclingContainer} starts the stateful instance with
 * {@code reversePort}).
 */
@Slf4j
@Component
public class CrossEnvDoclingBackend {

    /**
     * Well-known overlay port a Docling side-car publishes. Fixed to the DEFAULT container port:
     * the reverse relay publishes overlay-port == container-port, so an engine that customized
     * {@code sqs.docling.container-port} opts out of serving cross-environment callers (its local
     * conversions are unaffected).
     */
    public static final int DOCLING_OVERLAY_PORT = 5001;

    /** docling-jobkit's {@code INTERNAL_TASK_ERROR_MESSAGE} — the sanitised placeholder hiding the real cause. */
    private static final String SANITISED_TASK_ERROR = "Internal processing error";

    private final OverlayPeerDirectory peerDirectory;
    private final NetworkRuleStore networkRuleStore;
    private final DoclingImagePreprocessor imagePreprocessor;
    private final DoclingConvertSettings convertSettings;
    private final int convertTimeoutSeconds;
    private final int connectTimeoutSeconds;

    public CrossEnvDoclingBackend(OverlayPeerDirectory peerDirectory,
                                  NetworkRuleStore networkRuleStore,
                                  DoclingImagePreprocessor imagePreprocessor,
                                  DoclingConvertSettings convertSettings,
                                  @Value("${sqs.docling.convert-timeout-seconds:900}") int convertTimeoutSeconds,
                                  @Value("${sqs.docling.connect-timeout-seconds:5}") int connectTimeoutSeconds) {
        this.peerDirectory = peerDirectory;
        this.networkRuleStore = networkRuleStore;
        this.imagePreprocessor = imagePreprocessor;
        this.convertSettings = convertSettings;
        this.convertTimeoutSeconds = Math.max(1, convertTimeoutSeconds);
        this.connectTimeoutSeconds = Math.max(1, connectTimeoutSeconds);
    }

    /**
     * @param targetEnvId The sibling engine selected on the node ({@code target_env}); never blank/
     * {@code host} here — the delegate resolves those to the local backend
     * @throws IllegalStateException With an operator-actionable message when the route cannot work (no
     * rule, no such peer, target unreachable)
     */
    public String convert(String targetEnvId, byte[] bytes, String filename, String mimeType) {
        return convert(targetEnvId, bytes, filename, mimeType, null);
    }

    /**
     * Converts on {@code targetEnvId}'s Docling with the node's {@code output_format} carried
     * through — the target side-car is asked for the matching {@code to_formats}, so a remote
     * conversion honours the same markdown/text choice as a local one.
     */
    public String convert(String targetEnvId, byte[] bytes, String filename, String mimeType,
                          String outputFormat) {
        // Caller-side mirror check: the BE pushes this engine's outbound mirror when the TARGET
        // adds the inbound rule, so a missing mirror means the target has not allowed us — say
        // that, instead of spending a connect timeout discovering it.
        boolean outboundAllowed = networkRuleStore.outboundSnapshot().stream()
                .anyMatch(r -> targetEnvId.equals(r.targetEnvId()) && r.port() == DOCLING_OVERLAY_PORT);
        if (!outboundAllowed) {
            throw new IllegalStateException("engine " + targetEnvId + " does not allow this engine "
                    + "on port " + DOCLING_OVERLAY_PORT + " — add the inbound Network Access Control "
                    + "rule in the TARGET engine's Settings → Access & Security (source: this engine, "
                    + "port " + DOCLING_OVERLAY_PORT + ")");
        }
        Optional<String> overlayAddress = peerDirectory.addressOf(targetEnvId);
        if (overlayAddress.isEmpty()) {
            throw new IllegalStateException("engine " + targetEnvId + " is not reachable on the "
                    + "overlay — it is not an online same-owner peer of this engine "
                    + "(Error_PeerUnreachable: HostName env-" + targetEnvId
                    + " missing or offline in the local mesh map)");
        }
        // Downscale oversized bitmaps on the CALLER side, once — OCR cost scales with pixels and
        // the overlay hop should carry the prepared payload, not the raw one. Non-throwing: a bad
        // image still reaches convert with the original bytes.
        DoclingImagePreprocessor.PreparedDocument prepared =
                imagePreprocessor.prepare(bytes, filename, mimeType);
        String peerBaseUrl = "http://" + overlayAddress.get() + ":" + DOCLING_OVERLAY_PORT;
        // A peer-scoped client per call: conversions are seconds-to-minutes, so the construction
        // cost is noise, and nothing per-target needs caching between runs.
        DoclingClient peerClient = new DoclingClient(peerBaseUrl, convertTimeoutSeconds,
                connectTimeoutSeconds, convertSettings);
        // Names/ids only — never document bytes or converted text (REF-only logging invariant).
        log.info("docling: cross-env convert → engine={} via {} ({} bytes, name={})",
                targetEnvId, peerBaseUrl, prepared.bytes() == null ? 0 : prepared.bytes().length,
                prepared.filename());
        try {
            return peerClient.convert(prepared.bytes(), prepared.filename(), prepared.mimeType(),
                    outputFormat);
        } catch (DoclingConversionException failed) {
            // The target's side-car accepted the document and its conversion task FAILED — reachability, the
            // target's Docling entitlement and its inbound rule on port 5001 are all proven by the server
            // answering. The message is the server's own sanitised error (e.g. "whisper is not installed" for
            // a voice note on a side-car without the ASR layer); no connectivity checklist belongs here.
            throw new IllegalStateException("cross-environment Docling conversion on engine "
                    + targetEnvId + " failed on the target's Docling: " + failed.getMessage()
                    + sanitisedHint(failed.getMessage()), failed);
        } catch (RestClientResponseException served) {
            // Any HTTP error body still proves the request reached a serving Docling, so quote it
            // (bounded) rather than the enable/port advice — the old sync API's 404 "Task result
            // not found" masked failed conversions and sent operators chasing correct rules.
            throw new IllegalStateException("cross-environment Docling conversion on engine "
                    + targetEnvId + " was rejected by the target's Docling (" + peerBaseUrl + "): HTTP "
                    + served.getStatusCode().value() + " — "
                    + abbreviate(served.getResponseBodyAsString()), served);
        } catch (RuntimeException unreachable) {
            // Transport-level failure (connect timeout / refused / no route): the checklist applies.
            throw new IllegalStateException("cross-environment Docling conversion on engine "
                    + targetEnvId + " failed (" + peerBaseUrl + "): the target engine must have "
                    + "Docling enabled (its Engine Settings → General) and allow this engine on port "
                    + DOCLING_OVERLAY_PORT + " — " + unreachable.getMessage(), unreachable);
        }
    }

    /**
     * When the target answered with docling-serve's sanitised placeholder, point the operator at the
     * one place the real traceback exists: the target's side-car container logs. Current builds stamp
     * {@code DOCLING_SERVE_DEBUG_ERROR_DETAILS=true} on the managed side-car, so a current target
     * returns the real error instead of the placeholder.
     */
    private static String sanitisedHint(String serverMessage) {
        if (serverMessage == null || !serverMessage.contains(SANITISED_TASK_ERROR)) {
            return "";
        }
        return " — the target sanitised the real cause; fetch its sq-stateful-docling container "
                + "logs (stateful-instance-logs command) for the traceback";
    }

    /** Bounds a quoted error body so a hostile/broken peer cannot flood the node log. */
    private static String abbreviate(String body) {
        if (body == null || body.isBlank()) {
            return "<empty response body>";
        }
        String trimmed = body.trim();
        return trimmed.length() <= 300 ? trimmed : trimmed.substring(0, 300) + "…";
    }
}
