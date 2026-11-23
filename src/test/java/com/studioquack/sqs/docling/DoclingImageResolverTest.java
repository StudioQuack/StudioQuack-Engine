package com.studioquack.sqs.docling;

import com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DoclingImageResolver} — the platform contract: CPU multi-arch everywhere by default,
 * CUDA only on probed GPU passthrough, explicit operator overrides never second-guessed.
 */
class DoclingImageResolverTest {

    /** Probe double — GpuCapabilityProbe is not an interface, so override the single method. */
    private static GpuCapabilityProbe probeReturning(boolean available) {
        return new GpuCapabilityProbe(null, new com.fasterxml.jackson.databind.ObjectMapper()) {
            @Override
            public boolean gpuPassthroughAvailable() {
                return available;
            }
        };
    }

    @Test
    void auto_resolution_lands_on_the_multi_arch_cpu_image_without_a_probe() {
        DoclingImageResolver resolver = new DoclingImageResolver(null, DoclingImageResolver.DEFAULT_GPU_IMAGE);

        DoclingImageResolver.DoclingPlatform platform = resolver.resolve("", "", -1);

        assertEquals(DoclingImageResolver.CPU_IMAGE, platform.image(),
                "no probe = no GPU = the multi-arch CPU image (amd64/arm64 resolved by Podman)");
        assertEquals("cpu", platform.device());
        assertEquals(0, platform.gpus());
    }

    @Test
    void auto_resolution_lands_on_the_cpu_image_when_the_probe_says_no_passthrough() {
        DoclingImageResolver resolver =
                new DoclingImageResolver(probeReturning(false), DoclingImageResolver.DEFAULT_GPU_IMAGE);

        DoclingImageResolver.DoclingPlatform platform = resolver.resolve(null, null, -1);

        assertEquals(DoclingImageResolver.CPU_IMAGE, platform.image());
        assertEquals("cpu", platform.device());
        assertEquals(0, platform.gpus());
    }

    @Test
    void probed_gpu_passthrough_selects_the_pinned_cuda_image_device_and_one_gpu() {
        DoclingImageResolver resolver =
                new DoclingImageResolver(probeReturning(true), DoclingImageResolver.DEFAULT_GPU_IMAGE);

        DoclingImageResolver.DoclingPlatform platform = resolver.resolve("", "", -1);

        assertEquals(DoclingImageResolver.DEFAULT_GPU_IMAGE, platform.image(),
                "CUDA variant when the runtime can honour --gpus");
        assertTrue(platform.image().contains("-cu130:v"),
                "cu* images have NO latest tag upstream — the default must stay version-pinned: "
                        + platform.image());
        assertEquals("cuda", platform.device(),
                "the GPU image is useless without DOCLING_DEVICE=cuda");
        assertEquals(1, platform.gpus());
    }

    @Test
    void explicit_image_is_used_verbatim_even_with_gpu_passthrough() {
        DoclingImageResolver resolver =
                new DoclingImageResolver(probeReturning(true), DoclingImageResolver.DEFAULT_GPU_IMAGE);

        DoclingImageResolver.DoclingPlatform platform =
                resolver.resolve("quay.io/acme/docling-serve-cpu:pinned", "", -1);

        assertEquals("quay.io/acme/docling-serve-cpu:pinned", platform.image(),
                "operator intent is never second-guessed");
        assertEquals("cpu", platform.device(), "blank device with an explicit image stays cpu");
        assertEquals(0, platform.gpus());
    }

    @Test
    void explicit_device_and_gpu_count_win_in_auto_mode() {
        DoclingImageResolver resolver =
                new DoclingImageResolver(probeReturning(true), DoclingImageResolver.DEFAULT_GPU_IMAGE);

        DoclingImageResolver.DoclingPlatform platform = resolver.resolve("", "cuda", 2);

        assertEquals(2, platform.gpus(), "an explicit sqs.docling.gpus is honoured as-is");
        assertEquals("cuda", platform.device());
    }
}
