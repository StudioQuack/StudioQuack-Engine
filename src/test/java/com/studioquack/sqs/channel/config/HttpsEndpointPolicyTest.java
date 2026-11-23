package com.studioquack.sqs.channel.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * HTTPS is required for StudioQuack endpoints. Plain HTTP is only loopback.
 */
class HttpsEndpointPolicyTest {

    @Test
    @DisplayName("public https is accepted, including the production and DEV hostnames")
    void httpsAccepted() {
        assertThat(HttpsEndpointPolicy.refusal("https://api.studioquack.com")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("https://dev-api.studioquack.com")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("https://auth.studioquack.com/auth")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("https://dev-net.studioquack.com")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("https://192.0.2.10")).isNull();
    }

    @Test
    @DisplayName("blank is not this policy's problem")
    void blankIsUnset() {
        assertThat(HttpsEndpointPolicy.refusal(null)).isNull();
        assertThat(HttpsEndpointPolicy.refusal("")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("   ")).isNull();
    }

    @Test
    @DisplayName("http is allowed only for localhost, 127.0.0.1 and ::1")
    void loopbackHttpAllowed() {
        assertThat(HttpsEndpointPolicy.refusal("http://localhost:8443")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("http://127.0.0.1:8080")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("http://[::1]:8443")).isNull();
        assertThat(HttpsEndpointPolicy.refusal("http://localhost.")).isNull();
    }

    @Test
    @DisplayName("non-https to any other host fails closed, including RFC1918 and documentation IPs")
    void nonHttpsRefused() {
        assertThat(HttpsEndpointPolicy.refusal("http://api.studioquack.com"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("http://192.0.2.10"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("http://10.0.0.5:8080"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("http://192.168.1.20:8080"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("http://127.0.0.2"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("http://localhost.evil.com"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThat(HttpsEndpointPolicy.refusal("not a url :://"))
                .startsWith(HttpsEndpointPolicy.REFUSAL);
        assertThatThrownBy(() -> HttpsEndpointPolicy.require(
                "http://192.0.2.10", "sqs.backend.base-url"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(HttpsEndpointPolicy.REFUSAL)
                .hasMessageContaining("sqs.backend.base-url");
    }
}
