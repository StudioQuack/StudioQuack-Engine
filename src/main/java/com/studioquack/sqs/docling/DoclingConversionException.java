package com.studioquack.sqs.docling;

/**
 * The serving Docling engine accepted the document and reported the conversion task FAILED.
 * Carries the server's own public error message (docling-serve sanitises it through
 * {@code build_public_task_error}), so a node failure names the real cause — "whisper is not
 * installed", an unreadable file, an unsupported format.
 */
public class DoclingConversionException extends IllegalStateException {

    public DoclingConversionException(String message) {
        super(message);
    }

    public DoclingConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
