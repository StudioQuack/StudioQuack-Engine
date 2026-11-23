package com.studioquack.sqs.sandbox.warmpool;

/**
 * One image-keyed warm-pool target: which OCI image to hold warm, how many slots, and the
 * per-image resource caps applied to every container of that image (warm and cold).
 *
 * @param imageRef The pool's identity key — the resolved OCI image ref the reservoir holds wa.
 * @param language Optional hint that keeps the four script languages on the runner/broker eli.
 */
public record WarmTargetEntry(String imageRef, String language, int target, Caps caps) {

    /**
     * Per-image resource caps. Any field may be {@code null} (unset); {@link #isEmpty()} is true when
     * every field is unset.
     */
    public record Caps(Double cpu, Integer ramMb, Integer diskMb, Integer gpu) {
        public boolean isEmpty() { return cpu == null && ramMb == null && diskMb == null && gpu == null; }
        public double cpusOrZero() { return cpu == null ? 0.0 : cpu; }
        public int ramMbOrZero() { return ramMb == null ? 0 : ramMb; }
        public int diskMbOrZero() { return diskMb == null ? 0 : diskMb; }
        public int gpuOrZero() { return gpu == null ? 0 : gpu; }

        /** Back-compat: older shape (no gpu). */
        public Caps(Double cpu, Integer ramMb, Integer diskMb) {
            this(cpu, ramMb, diskMb, null);
        }
    }

    public WarmTargetEntry {
        imageRef = imageRef == null ? null : imageRef.trim();
        language = (language == null || language.isBlank()) ? null : language.trim().toLowerCase(java.util.Locale.ROOT);
        target = Math.max(0, target);
        // caps: null means "no caps configured" (stays uncapped — the legacy language-pool posture)
    }
}
