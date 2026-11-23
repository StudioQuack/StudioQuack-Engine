package com.studioquack.sqs.docling;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure-JUnit coverage for {@link DoclingImagePreprocessor}: pass-through for non-images / small
 * images / disabled ceiling / corrupt bytes, and proportional downscale of oversized bitmaps to PNG.
 */
class DoclingImagePreprocessorTest {

    @Test
    @DisplayName("non-image (PDF) bytes pass through unchanged")
    void nonImagePdfUnchanged() {
        DoclingImagePreprocessor prep =
                new DoclingImagePreprocessor(DoclingImagePreprocessor.DEFAULT_MAX_IMAGE_LONG_EDGE_PX);
        byte[] pdf = "%PDF-1.4 fake content for pass-through".getBytes();
        DoclingImagePreprocessor.PreparedDocument out =
                prep.prepare(pdf, "report.pdf", "application/pdf");

        assertSame(pdf, out.bytes());
        assertArrayEquals(pdf, out.bytes());
        assertEquals("report.pdf", out.filename());
        assertEquals("application/pdf", out.mimeType());
    }

    @Test
    @DisplayName("small PNG under the ceiling is returned unchanged")
    void smallPngUnderCeilingUnchanged() throws IOException {
        DoclingImagePreprocessor prep =
                new DoclingImagePreprocessor(DoclingImagePreprocessor.DEFAULT_MAX_IMAGE_LONG_EDGE_PX);
        byte[] png = pngBytes(400, 300);

        DoclingImagePreprocessor.PreparedDocument out =
                prep.prepare(png, "scan.png", "image/png");

        assertSame(png, out.bytes());
        assertEquals("scan.png", out.filename());
        assertEquals("image/png", out.mimeType());
    }

    @Test
    @DisplayName("oversized PNG is downscaled so long edge equals the ceiling and mime becomes image/png")
    void largePngIsDownscaledToCeiling() throws IOException {
        DoclingImagePreprocessor prep =
                new DoclingImagePreprocessor(DoclingImagePreprocessor.DEFAULT_MAX_IMAGE_LONG_EDGE_PX);
        byte[] png = pngBytes(3600, 2400);

        DoclingImagePreprocessor.PreparedDocument out =
                prep.prepare(png, "huge-scan.jpg", "image/jpeg");

        assertEquals("image/png", out.mimeType());
        assertEquals("huge-scan.png", out.filename());
        BufferedImage scaled = ImageIO.read(new ByteArrayInputStream(out.bytes()));
        assertEquals(1800, Math.max(scaled.getWidth(), scaled.getHeight()));
        assertEquals(1200, Math.min(scaled.getWidth(), scaled.getHeight()));
        assertTrue(out.bytes().length > 0);
        // Must not be the original byte array after a resize.
        assertTrue(!Arrays.equals(png, out.bytes()) || png.length != out.bytes().length
                || png != out.bytes());
    }

    @Test
    @DisplayName("ceiling 0 disables downscale entirely")
    void ceilingZeroDisablesDownscale() throws IOException {
        DoclingImagePreprocessor prep = new DoclingImagePreprocessor(0);
        byte[] png = pngBytes(3600, 2400);

        DoclingImagePreprocessor.PreparedDocument out =
                prep.prepare(png, "huge.gif", "image/gif");

        assertSame(png, out.bytes());
        assertEquals("huge.gif", out.filename());
        assertEquals("image/gif", out.mimeType());
    }

    @Test
    @DisplayName("corrupt / non-decodable image bytes pass through without throw")
    void corruptImageBytesPassThrough() {
        DoclingImagePreprocessor prep =
                new DoclingImagePreprocessor(DoclingImagePreprocessor.DEFAULT_MAX_IMAGE_LONG_EDGE_PX);
        byte[] garbage = new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05};

        DoclingImagePreprocessor.PreparedDocument out = assertDoesNotThrow(
                () -> prep.prepare(garbage, "broken.png", "image/png"));

        assertSame(garbage, out.bytes());
        assertEquals("broken.png", out.filename());
        assertEquals("image/png", out.mimeType());
    }

    /** Synthetic solid-colour PNG of the given dimensions (ImageIO only — no Maven deps). */
    private static byte[] pngBytes(int width, int height) throws IOException {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        try {
            g.setColor(Color.LIGHT_GRAY);
            g.fillRect(0, 0, width, height);
            g.setColor(Color.DARK_GRAY);
            g.drawRect(2, 2, width - 5, height - 5);
        } finally {
            g.dispose();
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(ImageIO.write(img, "png", out), "test fixture PNG encode must succeed");
        return out.toByteArray();
    }
}
