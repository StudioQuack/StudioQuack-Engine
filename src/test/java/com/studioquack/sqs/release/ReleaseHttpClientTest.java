package com.studioquack.sqs.release;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReleaseHttpClientTest {

    @Test
    void requiresProBearer_onlyOnSqsProPaths() {
        assertFalse(ReleaseHttpClient.requiresProBearer(
                URI.create("https://api.example.com/releases/client/release-manifest.json")));
        assertFalse(ReleaseHttpClient.requiresProBearer(
                URI.create("https://api.example.com/releases/client/sqs/windows/1.0.0/boot.jar")));
        assertTrue(ReleaseHttpClient.requiresProBearer(
                URI.create("https://api.example.com/releases/client/sqs-pro/linux/1.0.0/boot.jar")));
    }

    @Test
    void proPathWithoutBearerFailsFast() {
        ReleaseHttpClient client = ReleaseHttpClient.forTests(HttpClient.newHttpClient());
        assertThrows(IllegalStateException.class, () -> client.get(
                URI.create("https://api.example.com/releases/client/sqs-pro/linux/x.jar"),
                Duration.ofSeconds(5)));
    }

    @Test
    void proPathAddsBearerHeader() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setBearer("test-bearer");
        ReleaseHttpClient client = ReleaseHttpClient.forTests(props, HttpClient.newHttpClient());
        var req = client.get(
                URI.create("https://api.example.com/releases/client/sqs-pro/linux/x.jar"),
                Duration.ofSeconds(5)).build();
        assertTrue(req.headers().firstValue("Authorization")
                .filter(v -> v.equals("Bearer test-bearer"))
                .isPresent());
    }

    @Test
    void proPathNamesTheEnrolledEnvironment_soThePostEnrolmentProofCanBeChecked() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setBearer("installer-bearer");
        props.setEnvId("env-pro-1");
        props.installChannelCredential("sqikm1.proof");
        ReleaseHttpClient client = ReleaseHttpClient.forTests(props, HttpClient.newHttpClient());
        var req = client.get(
                URI.create("https://api.example.com/releases/client/sqs-pro/linux/x.jar"),
                Duration.ofSeconds(5)).build();
        assertTrue(req.headers().firstValue("Authorization").filter("Bearer sqikm1.proof"::equals).isPresent());
        assertTrue(req.headers().firstValue(ReleaseHttpClient.ENV_ID_HEADER).filter("env-pro-1"::equals).isPresent());
        var anonymous = client.get(
                URI.create("https://api.example.com/releases/client/sqs/linux/x.jar"), Duration.ofSeconds(5)).build();
        assertFalse(anonymous.headers().firstValue(ReleaseHttpClient.ENV_ID_HEADER).isPresent(),
                "self-hosted artifacts stay anonymous");
    }
}
