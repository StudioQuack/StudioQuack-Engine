package com.studioquack.sqs.release;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReleaseSignatureVerifierTest {

    /** Current release SPKI. The private half is not in this repo, so a matching signature cannot be minted here. */
    private static final String ROTATED_SPKI =
            "MCowBQYDK2VwAyEAFlcGLgDzkqL9aaiBi6FZw1T3UdswnliXSZqZK9ytp4s=";
    /** Disclosed pre-rotation SPKI. Must not be pinned. */
    private static final String DISCLOSED_SPKI =
            "MCowBQYDK2VwAyEAxFt556xIA9ImPY0X/T0bzFd6JrcmdZtdOcY+OPjowHA=";

    private static final byte[] MANIFEST =
            "{\"file\":\"lib.tar.gz\",\"sha256\":\"ab12\"}".getBytes(StandardCharsets.UTF_8);

    private static KeyPair keyPair() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static String pub(KeyPair kp) {
        return Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
    }

    /** Same shape the release script writes: base64 of the raw signature, trailing newline. */
    private static byte[] sigFile(KeyPair kp, byte[] data) throws Exception {
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(kp.getPrivate());
        s.update(data);
        return (Base64.getEncoder().encodeToString(s.sign()) + "\n").getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void validSignature_isAccepted_evenWhenRequired() throws Exception {
        KeyPair kp = keyPair();
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(pub(kp)), true);
        assertDoesNotThrow(() -> v.verify("m", MANIFEST, sigFile(kp, MANIFEST)));
    }

    @Test
    void tamperedManifest_isRefused_inTransitionToo() throws Exception {
        KeyPair kp = keyPair();
        byte[] sig = sigFile(kp, MANIFEST);
        byte[] swapped = "{\"file\":\"lib.tar.gz\",\"sha256\":\"ff00\"}".getBytes(StandardCharsets.UTF_8);
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(pub(kp)), false);
        assertThrows(ReleaseSignatureException.class, () -> v.verify("m", swapped, sig));
    }

    @Test
    void signatureFromAnotherKey_isRefused() throws Exception {
        KeyPair pinned = keyPair();
        KeyPair attacker = keyPair();
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(pub(pinned)), false);
        assertThrows(ReleaseSignatureException.class, () -> v.verify("m", MANIFEST, sigFile(attacker, MANIFEST)));
    }

    @Test
    void rotation_acceptsEitherPinnedKey() throws Exception {
        KeyPair oldKey = keyPair();
        KeyPair newKey = keyPair();
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(pub(oldKey), pub(newKey)), true);
        assertDoesNotThrow(() -> v.verify("m", MANIFEST, sigFile(newKey, MANIFEST)));
    }

    @Test
    void garbageSignature_isRefused() throws Exception {
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(pub(keyPair())), false);
        assertThrows(ReleaseSignatureException.class,
                () -> v.verify("m", MANIFEST, "not*base64!".getBytes(StandardCharsets.US_ASCII)));
        assertThrows(ReleaseSignatureException.class,
                () -> v.verify("m", MANIFEST, "AAAA".getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void productionPin_isOnlyTheRotatedKey_andRejectsUnsignedAndOtherKeys() throws Exception {
        assertEquals(List.of(ROTATED_SPKI), ReleaseSignatureVerifier.PINNED_KEYS);
        assertFalse(ReleaseSignatureVerifier.PINNED_KEYS.contains(DISCLOSED_SPKI));
        var value = ReleaseSignatureVerifier.class.getConstructor(boolean.class)
                .getParameters()[0]
                .getAnnotation(org.springframework.beans.factory.annotation.Value.class);
        assertEquals("${sqs.release.signing.required:true}", value.value());

        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(true);
        assertDoesNotThrow(() -> new ReleaseSignatureVerifier(List.of(ROTATED_SPKI), true));
        assertThrows(ReleaseSignatureException.class, () -> v.verify("m", MANIFEST, null));
        assertThrows(ReleaseSignatureException.class,
                () -> v.verify("m", MANIFEST, sigFile(keyPair(), MANIFEST)));
        assertThrows(ReleaseSignatureException.class,
                () -> v.verify("m", MANIFEST, "AAAA\n".getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void unsigned_isAcceptedInTransition_refusedWhenRequired() throws Exception {
        String key = pub(keyPair());
        assertDoesNotThrow(() -> new ReleaseSignatureVerifier(List.of(key), false).verify("m", MANIFEST, null));
        assertDoesNotThrow(() -> ReleaseSignatureVerifier.transitional().verify("m", MANIFEST, null));
        assertThrows(ReleaseSignatureException.class,
                () -> new ReleaseSignatureVerifier(List.of(key), true).verify("m", MANIFEST, null));
    }

    @Test
    void requiredWithNoPinnedKey_failsClosed() {
        ReleaseSignatureVerifier v = new ReleaseSignatureVerifier(List.of(), true);
        assertThrows(ReleaseSignatureException.class,
                () -> v.verify("m", MANIFEST, "AAAA".getBytes(StandardCharsets.US_ASCII)));
    }
}
