package com.studioquack.sqs.docling;

import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DoclingAsrImageBuilderTest {

    private final StatefulInstanceManager manager = mock(StatefulInstanceManager.class);

    @Test
    void asrDisabledReturnsTheBaseImageUntouched() {
        DoclingAsrImageBuilder builder = new DoclingAsrImageBuilder(manager, false, 900);

        assertEquals("quay.io/docling-project/docling-serve-cpu:latest",
                builder.resolveRuntimeImage("quay.io/docling-project/docling-serve-cpu:latest"));
        verify(manager, never()).imageExists(any());
        verify(manager, never()).buildImage(any(), any(), any(), anyBoolean());
    }

    @Test
    void existingDerivedImageShortCircuitsTheBuild() {
        String derived = DoclingAsrImageBuilder.derivedRefFor("quay.io/docling-project/docling-serve-cpu:latest");
        when(manager.imageExists(derived)).thenReturn(true);
        DoclingAsrImageBuilder builder = new DoclingAsrImageBuilder(manager, true, 900);

        assertEquals(derived, builder.resolveRuntimeImage("quay.io/docling-project/docling-serve-cpu:latest"));
        verify(manager, never()).buildImage(any(), any(), any(), anyBoolean());
    }

    @Test
    void missingDerivedImageBuildsItFromTheBase() {
        String base = "quay.io/docling-project/docling-serve-cpu:latest";
        String derived = DoclingAsrImageBuilder.derivedRefFor(base);
        when(manager.imageExists(derived)).thenReturn(false);
        // eq(true): the daemon-trusted build runs on the host network — a private build
        // namespace's DNS breaks on stub-resolver hosts (DGX OS) while the host resolves fine.
        when(manager.buildImage(eq(derived), any(Path.class), any(Duration.class), eq(true)))
                .thenAnswer(invocation -> {
                    Path containerFile = invocation.getArgument(1);
                    String content = Files.readString(containerFile);
                    assertTrue(content.contains("FROM " + base),
                            "the derived image must extend the resolved base image");
                    assertTrue(content.contains("openai-whisper"),
                            "the ASR layer installs the whisper runtime");
                    assertTrue(content.contains("imageio-ffmpeg"),
                            "the ASR layer brings a static ffmpeg (webm/opus decode)");
                    assertTrue(content.contains("whisper.load_model('tiny')"),
                            "the default whisper-tiny checkpoint is baked at build time");
                    return new StatefulInstanceManager.LoadResult(derived, "sha256:abc", StatefulStatus.ACTIVE);
                });
        DoclingAsrImageBuilder builder = new DoclingAsrImageBuilder(manager, true, 900);

        assertEquals(derived, builder.resolveRuntimeImage(base));
    }

    @Test
    void failedBuildFallsBackToTheBaseImage() {
        String base = "quay.io/docling-project/docling-serve-cpu:latest";
        String derived = DoclingAsrImageBuilder.derivedRefFor(base);
        when(manager.imageExists(derived)).thenReturn(false);
        when(manager.buildImage(any(), any(), any(), anyBoolean()))
                .thenReturn(new StatefulInstanceManager.LoadResult(derived, null, StatefulStatus.FAILED));
        DoclingAsrImageBuilder builder = new DoclingAsrImageBuilder(manager, true, 900);

        assertEquals(base, builder.resolveRuntimeImage(base),
                "document conversion must not be held hostage by a failed voice add-on build");
    }

    @Test
    void derivedRefCarriesTheBaseTagAndRecipeRevision() {
        assertEquals("localhost/sq-docling-serve-asr:latest-asr1",
                DoclingAsrImageBuilder.derivedRefFor("quay.io/docling-project/docling-serve-cpu:latest"));
        assertEquals("localhost/sq-docling-serve-asr:v1.18.0-asr1",
                DoclingAsrImageBuilder.derivedRefFor("quay.io/docling-project/docling-serve-cu130:v1.18.0"));
        assertEquals("localhost/sq-docling-serve-asr:latest-asr1",
                DoclingAsrImageBuilder.derivedRefFor("quay.io/docling-project/docling-serve-cpu"));
    }

    private static final class Files {
        static String readString(Path path) {
            try {
                return java.nio.file.Files.readString(path);
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
