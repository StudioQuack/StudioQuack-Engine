package com.studioquack.sqs.docling;

import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * ASR (voice/audio transcription) layer for the daemon-managed Docling side-car. The stock
 * {@code docling-serve} images install {@code docling-slim} WITHOUT its {@code format-audio}/
 * {@code format-video} extras and without an {@code ffmpeg} binary, so an audio upload (a DuckBot
 * voice note, {@code.webm}) fails.
 */
@Slf4j
@Component
public class DoclingAsrImageBuilder {

    /** Local repository for the derived image. */
    static final String DERIVED_REPO = "localhost/sq-docling-serve-asr";

    /**
     * Recipe revision — bump when the Containerfile below changes so existing hosts rebuild
     * instead of reusing a stale derived layer.
     */
    static final String RECIPE_REVISION = "asr1";

    /**
     * {@code %s} is the resolved base image ref. Installs into the upstream virtualenv (
     * {@code /opt/app-root}) via a copied {@code uv} binary — the base image is built with uv and
     * ships no pip.
     */
    static final String CONTAINERFILE = """
            # StudioQuack ASR layer — voice/audio transcription for the Docling side-car.
            # Stock docling-serve ships docling-slim without format-audio/format-video extras,
            # so audio uploads fail with "whisper is not installed".
            FROM %s
            USER 0
            COPY --from=ghcr.io/astral-sh/uv:0.8.19 /uv /usr/local/bin/uv
            RUN uv pip install --python /opt/app-root/bin/python --no-cache \\
                    "openai-whisper>=20250625" imageio-ffmpeg \\
                && ln -sf "$(/opt/app-root/bin/python -c 'import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())')" \\
                    /usr/local/bin/ffmpeg
            USER 1001
            RUN /opt/app-root/bin/python -c "import whisper; whisper.load_model('tiny')"
            """;

    private final StatefulInstanceManager statefulInstanceManager;
    private final boolean asrEnabled;
    private final Duration buildTimeout;

    public DoclingAsrImageBuilder(
            StatefulInstanceManager statefulInstanceManager,
            @Value("${sqs.docling.asr.enabled:true}") boolean asrEnabled,
            // WHY 900: openai-whisper pulls a triton wheel (~250 MB) plus the tiny checkpoint
            // (~75 MB); the base image itself is already local by the time we build.
            @Value("${sqs.docling.asr-build-timeout-seconds:900}") long buildTimeoutSeconds) {
        this.statefulInstanceManager = statefulInstanceManager;
        this.asrEnabled = asrEnabled;
        this.buildTimeout = Duration.ofSeconds(Math.max(120, buildTimeoutSeconds));
    }

    /**
     * The image the side-car should actually start. With ASR off this is the base image; otherwise the
     * derived ASR image, building it on first use.
     */
    public String resolveRuntimeImage(String baseImage) {
        if (!asrEnabled) {
            return baseImage;
        }
        String derivedRef = derivedRefFor(baseImage);
        if (statefulInstanceManager.imageExists(derivedRef)) {
            return derivedRef;
        }
        log.info("docling: building the ASR layer image {} from {} (one-time per host; budget {}s) "
                + "— adds openai-whisper + a static ffmpeg so voice/audio files transcribe",
                derivedRef, baseImage, buildTimeout.getSeconds());
        try {
            Path dir = Files.createTempDirectory("sq-docling-asr-");
            Path containerFile = dir.resolve("Containerfile");
            Files.writeString(containerFile, CONTAINERFILE.formatted(baseImage));
            // hostNetwork: the build's RUN steps fetch from PyPI/OpenAI CDN, and a private build
            // namespace's DNS breaks on hosts with stub-resolver forwarding (DGX OS) even when the
            // host itself resolves fine. The Containerfile is daemon-fixed and trusted.
            StatefulInstanceManager.LoadResult build =
                    statefulInstanceManager.buildImage(derivedRef, containerFile, buildTimeout, true);
            if (StatefulStatus.ACTIVE.equals(build.status())) {
                log.info("docling: ASR layer image ready ref={} digest={}", derivedRef, build.digest());
                return derivedRef;
            }
        } catch (Exception e) {
            log.warn("docling: ASR layer build failed for base {}: {}", baseImage, e.toString());
        }
        log.warn("docling: falling back to the base image {} — document conversion is unaffected, "
                + "but audio/video files (voice notes) will fail until the ASR layer builds",
                baseImage);
        return baseImage;
    }

    /** {@code quay.io/docling-project/docling-serve-cpu:latest} → {@code localhost/…:latest-asr1}. */
    static String derivedRefFor(String baseImage) {
        String tag = "latest";
        if (baseImage != null) {
            int colon = baseImage.lastIndexOf(':');
            int slash = baseImage.lastIndexOf('/');
            if (colon > slash && colon < baseImage.length() - 1) {
                tag = baseImage.substring(colon + 1);
            }
        }
        String sanitized = tag.replaceAll("[^a-zA-Z0-9_.-]", "-");
        return DERIVED_REPO + ":" + sanitized + "-" + RECIPE_REVISION;
    }
}
