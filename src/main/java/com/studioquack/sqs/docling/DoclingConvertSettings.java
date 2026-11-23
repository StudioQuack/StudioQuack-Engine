package com.studioquack.sqs.docling;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Defaults favour interactive latency over docling-serve's accuracy-oriented stock settings
 * (TableFormer on, {@code images_scale=2.0}, embedded picture bytes). Each field maps 1:1 to a
 * docling-serve {@code ConvertDocumentsOptions} form field.
 */
@Component
public class DoclingConvertSettings {

    /** Images/scans need OCR; digital PDFs still benefit when pages are image-only. */
    public final boolean doOcr;
    /** Re-OCR of already-digital text is expensive and usually worse — leave off. */
    public final boolean forceOcr;
    /**
     * OCR engine / preset id posted as {@code ocr_engine} and {@code ocr_preset}. WHY tesseract:
     * Docling-integrated CPU benches are ~3–8× faster than RapidOCR for plain scans; the cpu image
     * already ships tesseract + eng tessdata.
     */
    public final String ocrEngine;
    /** TableFormer is a major CPU cost; off for interactive convert. */
    public final boolean doTableStructure;
    /** If tables are re-enabled, prefer speed over accuracy. */
    public final String tableMode;
    /** Docling default 2.0 is 4× pixels; 1.0 keeps OCR workable without the pixel blow-up. */
    public final double imagesScale;
    /** Platform wants markdown text, not embedded picture bytes in the response. */
    public final boolean includeImages;
    /** Placeholder refs instead of inlining image payloads when images appear. */
    public final String imageExportMode;
    /** StudioQuack nodes consume markdown. */
    public final String toFormats;
    /** Enrichment passes are model-heavy; off for interactive convert. */
    public final boolean doCodeEnrichment;
    /** Enrichment passes are model-heavy; off for interactive convert. */
    public final boolean doFormulaEnrichment;
    /** Picture classification is model-heavy; off for interactive convert. */
    public final boolean doPictureClassification;
    /** Picture description is model-heavy; off for interactive convert. */
    public final boolean doPictureDescription;
    /** Chart extraction is model-heavy; off for interactive convert. */
    public final boolean doChartExtraction;

    public DoclingConvertSettings(
            @Value("${sqs.docling.do-ocr:true}") boolean doOcr,
            @Value("${sqs.docling.force-ocr:false}") boolean forceOcr,
            @Value("${sqs.docling.ocr-engine:tesseract}") String ocrEngine,
            @Value("${sqs.docling.do-table-structure:false}") boolean doTableStructure,
            @Value("${sqs.docling.table-mode:fast}") String tableMode,
            @Value("${sqs.docling.images-scale:1.0}") double imagesScale,
            @Value("${sqs.docling.include-images:false}") boolean includeImages,
            @Value("${sqs.docling.image-export-mode:placeholder}") String imageExportMode,
            @Value("${sqs.docling.to-formats:md}") String toFormats,
            @Value("${sqs.docling.do-code-enrichment:false}") boolean doCodeEnrichment,
            @Value("${sqs.docling.do-formula-enrichment:false}") boolean doFormulaEnrichment,
            @Value("${sqs.docling.do-picture-classification:false}") boolean doPictureClassification,
            @Value("${sqs.docling.do-picture-description:false}") boolean doPictureDescription,
            @Value("${sqs.docling.do-chart-extraction:false}") boolean doChartExtraction) {
        this.doOcr = doOcr;
        this.forceOcr = forceOcr;
        this.ocrEngine = ocrEngine;
        this.doTableStructure = doTableStructure;
        this.tableMode = tableMode;
        this.imagesScale = imagesScale;
        this.includeImages = includeImages;
        this.imageExportMode = imageExportMode;
        this.toFormats = toFormats;
        this.doCodeEnrichment = doCodeEnrichment;
        this.doFormulaEnrichment = doFormulaEnrichment;
        this.doPictureClassification = doPictureClassification;
        this.doPictureDescription = doPictureDescription;
        this.doChartExtraction = doChartExtraction;
    }

    /** Interactive latency defaults — used by tests and the 3-arg {@link DoclingClient} ctor. */
    public static DoclingConvertSettings interactiveDefaults() {
        return new DoclingConvertSettings(
                true, false, "tesseract", false, "fast", 1.0, false,
                "placeholder", "md",
                false, false, false, false, false);
    }

    /** Compact name=value list for construction logging (never document content). */
    String summarize() {
        return "do_ocr=" + doOcr
                + ", force_ocr=" + forceOcr
                + ", ocr_engine=" + ocrEngine
                + ", do_table_structure=" + doTableStructure
                + ", table_mode=" + tableMode
                + ", images_scale=" + imagesScale
                + ", include_images=" + includeImages
                + ", image_export_mode=" + imageExportMode
                + ", to_formats=" + toFormats
                + ", do_code_enrichment=" + doCodeEnrichment
                + ", do_formula_enrichment=" + doFormulaEnrichment
                + ", do_picture_classification=" + doPictureClassification
                + ", do_picture_description=" + doPictureDescription
                + ", do_chart_extraction=" + doChartExtraction;
    }
}
