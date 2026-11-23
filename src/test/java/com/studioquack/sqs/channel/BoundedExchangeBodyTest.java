package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedExchangeBodyTest {

    @Test
    void read_rejectsOversizedBody() {
        byte[] body = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        BoundedExchangeBody.TooLarge ex = assertThrows(BoundedExchangeBody.TooLarge.class,
                () -> BoundedExchangeBody.read(new ByteArrayInputStream(body), 4));
        assertTrue(ex.getMessage().contains("exceeded 4 bytes"));
    }

    @Test
    void read_acceptsBodyAtLimit() throws Exception {
        byte[] body = "abcd".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(body, BoundedExchangeBody.read(new ByteArrayInputStream(body), 4));
    }
}
