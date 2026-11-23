package com.studioquack.sqs.secrets.os;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link H2CipherPasswordEnvironmentPostProcessor} — the embedded database {@code
 * CIPHER=AES} file password wiring. Injects the test-only {@link InMemorySecretManager} via the
 * package-private factory seam so it stays hermetic and never touches a real platform keychain.
 */
class H2CipherPasswordEnvironmentPostProcessorTest {

    private static final String CIPHER_URL =
            "jdbc:h2:file:./data/sqs-engine;CIPHER=AES;AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE";

    private final InMemorySecretManager store = new InMemorySecretManager();
    private final H2CipherPasswordEnvironmentPostProcessor epp =
            new H2CipherPasswordEnvironmentPostProcessor(props -> store);

    private MockEnvironment cipherEnv(Path tmp) {
        MockEnvironment env = new MockEnvironment();
        env.setProperty("spring.datasource.url", CIPHER_URL);
        env.setProperty("sqs.secrets.os.data-dir", tmp.toString());
        return env;
    }

    @Test
    void injects_a_two_token_cipher_password_sourced_from_the_manager(@TempDir Path tmp) {
        MockEnvironment env = cipherEnv(tmp);

        epp.postProcessEnvironment(env, null);

        String pw = env.getProperty("spring.datasource.password");
        assertTrue(pw != null && pw.contains(" "), "H2 cipher password is two space-separated tokens");
        String[] parts = pw.split(" ");
        assertEquals(2, parts.length, "exactly <filePassword> <userPassword>");
        assertFalse(parts[0].isBlank(), "file password token present");
        assertFalse(parts[1].isBlank(), "user password token present");
    }

    @Test
    void honours_an_operator_supplied_user_password(@TempDir Path tmp) {
        MockEnvironment env = cipherEnv(tmp);
        env.setProperty("spring.datasource.password", "operatorUserPw"); // single token → the user password

        epp.postProcessEnvironment(env, null);

        String pw = env.getProperty("spring.datasource.password");
        assertTrue(pw.endsWith(" operatorUserPw"), "operator user password is preserved as the 2nd token");
        assertFalse(pw.startsWith(" "), "a file password is prepended as the 1st token");
    }

    @Test
    void is_a_no_op_for_a_plain_non_cipher_url(@TempDir Path tmp) {
        MockEnvironment env = new MockEnvironment();
        env.setProperty("spring.datasource.url", "jdbc:h2:mem:sqs-test;DB_CLOSE_DELAY=-1");

        epp.postProcessEnvironment(env, null);

        assertNull(env.getProperty("spring.datasource.password"),
                "no password is injected for a non-ciphered (test/in-memory) datasource");
    }

    @Test
    void is_a_no_op_when_operator_already_supplied_a_two_token_password(@TempDir Path tmp) {
        MockEnvironment env = cipherEnv(tmp);
        env.setProperty("spring.datasource.password", "preFile preUser"); // already two tokens

        epp.postProcessEnvironment(env, null);

        assertEquals("preFile preUser", env.getProperty("spring.datasource.password"),
                "an existing two-token password is left untouched");
    }
}
