package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SecretCommandHandlersTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void commitSecret_opensBearerEnvelopeAndStoresInOsSecretManager() throws Exception {
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId("env-1");
        channel.setBearer("sqvm_test_bearer");

        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setServicePrefix("com.studioquack.sqs.test");
        FakeSecrets secrets = new FakeSecrets();
        SecretStoreNamespace namespace = new SecretStoreNamespace(props, channel);

        ClientSecretInventoryService inventory = mock(ClientSecretInventoryService.class);
        var handler = new SecretCommandHandlers.CommitSecretCommandHandler(mapper, channel, secrets, namespace, inventory);
        String inner = """
                {"key":"chatgpt","mode":"env","plaintext":"sk-test"}
                """;
        SecureEnvelope sealed = EnvelopeCrypto.seal(
                EnvelopeCrypto.ikmFromBearer(channel.getBearer()),
                Direction.BE_TO_AGENT,
                channel.getEnvId(),
                SecretCommandHandlers.AAD_COMMIT,
                inner.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        DaemonCommand command = new DaemonCommand("cmd-1", "commit-secret", null, mapper.writeValueAsString(sealed));

        DaemonCommandOutcome out = handler.handle(command);

        assertEquals("ok", out.status());
        var response = mapper.readTree(out.output());
        assertTrue(response.get("secret_id").asText().matches("sec_[0-9a-f]{24}"));
        assertEquals(1, response.get("version").asInt());
        assertEquals(7, response.get("plaintext_length").asInt());

        String account = response.get("secret_id").asText() + ":v1";
        // Engine-scoped namespace: the entry lands under <prefix>.secret.<envId>, never the
        // host-shared legacy service a reinstall could inherit.
        assertEquals("sk-test", secrets.get("com.studioquack.sqs.test.secret.env-1", account).orElse(null));
        assertTrue(secrets.get("com.studioquack.sqs.test.secret", account).isEmpty());
        assertNotNull(response.get("plaintext_hash_b64").asText());

        // Ownership is recorded so a later uninstall purge can delete this entry.
        verify(inventory).recordCommit(response.get("secret_id").asText(), 1);
    }

    static class FakeSecrets implements OsSecretManager {
        private final Map<String, String> values = new ConcurrentHashMap<>();

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public Optional<String> get(String service, String account) {
            return Optional.ofNullable(values.get(service + "\0" + account));
        }

        @Override
        public void put(String service, String account, String secret) {
            values.put(service + "\0" + account, secret);
        }

        @Override
        public boolean delete(String service, String account) {
            return values.remove(service + "\0" + account) != null;
        }
    }
}
