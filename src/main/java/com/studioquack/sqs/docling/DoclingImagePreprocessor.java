package com.studioquack.sqs.docling;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Locale;
import java.util.Set;

/**
 * Shrinks oversized bitmaps before they are POSTed to the local docling-serve side-car. A
 * multi-megabyte GIF or scan can be tens of thousands of pixels on a side even when the compressed
 * file looks modest — the daemon owns those bytes, so it must downscale them before the convert
 * call rather than hoping the side-car will.
 */
@Slf4j
@Component
public class DoclingImagePreprocessor {

    /**
     * Default long-edge ceiling in pixels ({@code sqs.docling.max-image-long-edge-px}).
     * WHY 1800: ~letter-page width at ~200 DPI; enough for OCR; cuts pixels vs multi-k-px scans
     * roughly quadratically.
     */
    public static final int DEFAULT_MAX_IMAGE_LONG_EDGE_PX = 1800;

    private static final Set<String> IMAGE_EXTENSIONS = Set.of(
            "gif", "png", "jpg", "jpeg", "webp", "bmp", "tiff", "tif");

    private final int maxImageLongEdgePx;

    public DoclingImagePreprocessor(
            @Value("${sqs.docling.max-image-long-edge-px:1800}") int maxImageLongEdgePx) {
        this.maxImageLongEdgePx = maxImageLongEdgePx;
    }

    /** Bytes + identity ready for {@link DoclingClient#convert}. */
    public record PreparedDocument(byte[] bytes, String filename, String mimeType) {}

    /**
     * Prepare document bytes for convert. Images over the long-edge ceiling are scaled
     * proportionally and re-encoded as PNG; everything else is returned unchanged.
     */
    public PreparedDocument prepare(byte[] bytes, String filename, String mimeType) {
        if (bytes == null || !isImage(filename, mimeType)) {
            return new PreparedDocument(bytes, filename, mimeType);
        }
        // 0 / negative disables — ops escape hatch when a site needs native resolution.
        if (maxImageLongEdgePx <= 0) {
            return new PreparedDocument(bytes, filename, mimeType);
        }

        final BufferedImage image;
        try {
            image = ImageIO.read(new ByteArrayInputStream(bytes));
        } catch (Exception e) {
            // Never dump pixels/content — name/size/mime only.
            log.warn("docling: image decode failed; posting original bytes (name={}, size={}, mime={}): {}",
                    filename, bytes.length, mimeType, e.toString());
            return new PreparedDocument(bytes, filename, mimeType);
        }
        if (image == null) {
            log.warn("docling: image decode returned null; posting original bytes (name={}, size={}, mime={})",
                    filename, bytes.length, mimeType);
            return new PreparedDocument(bytes, filename, mimeType);
        }

        int srcW = image.getWidth();
        int srcH = image.getHeight();
        int longEdge = Math.max(srcW, srcH);
        if (longEdge <= maxImageLongEdgePx) {
            // Never upscale — under-ceiling bitmaps keep original encoding and bytes.
            return new PreparedDocument(bytes, filename, mimeType);
        }

        // Pin the long edge exactly to the ceiling so OCR pixel budget is deterministic.
        final int dstW;
        final int dstH;
        if (srcW >= srcH) {
            dstW = maxImageLongEdgePx;
            dstH = Math.max(1, (int) Math.round(srcH * ((double) maxImageLongEdgePx / srcW)));
        } else {
            dstH = maxImageLongEdgePx;
            dstW = Math.max(1, (int) Math.round(srcW * ((double) maxImageLongEdgePx / srcH)));
        }

        try {
            // TYPE_INT_RGB: OCR does not need alpha; avoids format surprises from indexed GIFs.
            BufferedImage scaled = new BufferedImage(dstW, dstH, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = scaled.createGraphics();
            try {
                g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                        RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                g.drawImage(image, 0, 0, dstW, dstH, null);
            } finally {
                g.dispose();
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // PNG after resize: stable, lossless for OCR, avoids re-animating GIF frames.
            if (!ImageIO.write(scaled, "png", out)) {
                log.warn("docling: PNG encode failed; posting original bytes (name={}, size={}, mime={})",
                        filename, bytes.length, mimeType);
                return new PreparedDocument(bytes, filename, mimeType);
            }
            byte[] pngBytes = out.toByteArray();
            String outFilename = rewriteExtensionToPng(filename);
            log.info("docling: downscaled image before convert name={} {}x{} ({} bytes) -> {}x{} ({} bytes) mime=image/png",
                    filename, srcW, srcH, bytes.length, dstW, dstH, pngBytes.length);
            return new PreparedDocument(pngBytes, outFilename, "image/png");
        } catch (Exception e) {
            log.warn("docling: image resize failed; posting original bytes (name={}, size={}, mime={}): {}",
                    filename, bytes.length, mimeType, e.toString());
            return new PreparedDocument(bytes, filename, mimeType);
        }
    }

    /** Image if mime is {@code image/*} OR the filename uses a common image extension. */
    static boolean isImage(String filename, String mimeType) {
        if (mimeType != null) {
            String m = mimeType.trim().toLowerCase(Locale.ROOT);
            if (m.startsWith("image/")) {
                return true;
            }
        }
        return hasImageExtension(filename);
    }

    private static boolean hasImageExtension(String filename) {
        String ext = extensionOf(filename);
        return ext != null && IMAGE_EXTENSIONS.contains(ext);
    }

    /**
     * Swap a known image extension for {@code .png}; leave non-image / extensionless names alone
     * so callers that keyed identity on the original name stay coherent.
     */
    static String rewriteExtensionToPng(String filename) {
        if (filename == null || filename.isBlank()) {
            return filename;
        }
        int dot = filename.lastIndexOf('.');
        if (dot <= 0 || dot == filename.length() - 1) {
            return filename;
        }
        String ext = filename.substring(dot + 1).toLowerCase(Locale.ROOT);
        if (!IMAGE_EXTENSIONS.contains(ext)) {
            return filename;
        }
        return filename.substring(0, dot) + ".png";
    }

    private static String extensionOf(String filename) {
        if (filename == null || filename.isBlank()) {
            return null;
        }
        int dot = filename.lastIndexOf('.');
        if (dot <= 0 || dot == filename.length() - 1) {
            return null;
        }
        return filename.substring(dot + 1).toLowerCase(Locale.ROOT);
    }
}
