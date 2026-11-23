package com.studioquack.sqs.sandbox.helperchan;

/**
 * Consumer of one helper-channel HTTP response, used by {@link HelperPipeChannel} to turn router
 * output into either a one-shot pipe frame or a sequence of streamed frames. The in-container
 * broker treats a one-shot frame {@code {id,status,body_b64}} as today's Content-Length reply
 * (byte-identical).
 */
public interface HelperStreamSink {

    /**
     * Single buffered body. The pipe writes a legacy {@code {id,status,body_b64}} frame;
     * {@code contentType} is recorded for the caller but MUST NOT appear on the one-shot
     * wire frame (today's broker ignores unknown fields, and we keep the three-field shape
     * byte-identical).
     */
    void oneShot(int status, String contentType, byte[] body);

    /** One streamed chunk. {@code seq} is 0-based and monotonic per request. */
    void chunk(int status, String contentType, byte[] chunk, int seq, boolean done);
}
