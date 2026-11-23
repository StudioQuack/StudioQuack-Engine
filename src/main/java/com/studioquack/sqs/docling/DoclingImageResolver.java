package com.studioquack.sqs.docling;

import com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Picks the Docling side-car image + accelerator posture for THIS host, so the daemon never
 * hard-codes one image for every platform ("cpu ise cpu, gpu ise gpu"): CPU (default) —
 * {@value #CPU_IMAGE}. Multi-arch manifest (linux/amd64 + linux/arm64), so Podman resolves the
 * right layer set for an Intel/AMD host, an Apple Silicon Podman machine, or a Windows ARM WSL VM
 * from the SAME ref — no per-arch branching is needed on our side.
 */
@Component
public class DoclingImageResolver {

    /** CPU variant — multi-arch (amd64 + arm64), tagged {@code latest}. ~4.4 GB. */
    public static final String CPU_IMAGE = "quay.io/docling-project/docling-serve-cpu:latest";


    public static final String DEFAULT_GPU_IMAGE = "quay.io/docling-project/docling-serve-cu130:v1.18.0";

    /** Null in tests / minimal assemblies: no probe means "no GPU passthrough" (safe default). */
    private final GpuCapabilityProbe gpuProbe;
    private final String gpuImage;

    public DoclingImageResolver(@Nullable GpuCapabilityProbe gpuProbe,
                                @Value("${sqs.docling.gpu-image:" + DEFAULT_GPU_IMAGE + "}") String gpuImage) {
        this.gpuProbe = gpuProbe;
        this.gpuImage = (gpuImage == null || gpuImage.isBlank()) ? DEFAULT_GPU_IMAGE : gpuImage.trim();
    }

    /** Effective image/device/gpu-count triple plus the human-readable reason it was chosen. */
    public record DoclingPlatform(String image, String device, int gpus, String reason) { }

    /**
     * Resolves the platform. Blank {@code explicitImage} means "auto"; a blank {@code explicitDevice}
     * follows the image choice (cuda with the GPU image, cpu otherwise); {@code explicitGpus < 0}
     * means "auto" (1 with the GPU image, else 0).
     */
    public DoclingPlatform resolve(String explicitImage, String explicitDevice, int explicitGpus) {
        if (explicitImage != null && !explicitImage.isBlank()) {
            String device = (explicitDevice == null || explicitDevice.isBlank())
                    ? "cpu" : explicitDevice.trim();
            int gpus = explicitGpus >= 0 ? explicitGpus : 0;
            return new DoclingPlatform(explicitImage.trim(), device, gpus,
                    "explicit sqs.docling.image override");
        }
        boolean gpu = gpuProbe != null && gpuProbe.gpuPassthroughAvailable();
        if (gpu) {
            String device = (explicitDevice == null || explicitDevice.isBlank())
                    ? "cuda" : explicitDevice.trim();
            int gpus = explicitGpus >= 0 ? explicitGpus : 1;
            return new DoclingPlatform(gpuImage, device, gpus,
                    "GPU passthrough detected (CDI) — CUDA variant " + gpuImage);
        }
        String device = (explicitDevice == null || explicitDevice.isBlank())
                ? "cpu" : explicitDevice.trim();
        int gpus = Math.max(explicitGpus, 0);
        return new DoclingPlatform(CPU_IMAGE, device, gpus,
                "no GPU passthrough — multi-arch CPU variant (amd64/arm64 resolved by Podman)");
    }
}
