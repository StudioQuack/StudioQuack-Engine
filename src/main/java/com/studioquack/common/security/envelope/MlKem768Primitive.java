package com.studioquack.common.security.envelope;

import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.SecretWithEncapsulation;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMExtractor;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMGenerator;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMKeyGenerationParameters;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMKeyPairGenerator;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMParameters;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMPrivateKeyParameters;
import org.bouncycastle.pqc.crypto.mlkem.MLKEMPublicKeyParameters;

import java.security.SecureRandom;

/**
 * ML-KEM-768 half of the hybrid KEM, via BouncyCastle.
 * Used once at enrolment; later envelopes use the derived IKM.
 */
public final class MlKem768Primitive implements HybridKemPrimitive {

    /** Stable wire identifier — referenced by KemSchema and AAD. */
    public static final String NAME = "ML-KEM-768";

    private static final int PUBLIC_KEY_LEN = 1184;
    private static final int CIPHERTEXT_LEN = 1088;

    private static final SecureRandom RANDOM = new SecureRandom();

    private static MLKEMParameters parameterSet() {
        // Fixed parameter set so the wire name and the provider stay aligned.
        return MLKEMParameters.ml_kem_768;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public int publicKeyLength() {
        return PUBLIC_KEY_LEN;
    }

    @Override
    public int ciphertextLength() {
        return CIPHERTEXT_LEN;
    }

    @Override
    public KemKeypair generateKeypair() {
        try {
            MLKEMKeyPairGenerator gen = new MLKEMKeyPairGenerator();
            gen.init(new MLKEMKeyGenerationParameters(RANDOM, parameterSet()));
            AsymmetricCipherKeyPair raw = gen.generateKeyPair();
            byte[] pub = ((MLKEMPublicKeyParameters) raw.getPublic()).getEncoded();
            byte[] priv = ((MLKEMPrivateKeyParameters) raw.getPrivate()).getEncoded();
            return new KemKeypair(pub, priv);
        } catch (Exception ex) {
            throw new KemException(NAME, "keypair generation failed", ex);
        }
    }

    @Override
    public KemEncapsulation encapsulate(byte[] peerPublicKey) {
        if (peerPublicKey == null || peerPublicKey.length != PUBLIC_KEY_LEN) {
            throw new KemException(NAME,
                    "peer public key has wrong length " +
                            (peerPublicKey == null ? "null" : String.valueOf(peerPublicKey.length)),
                    null);
        }
        try {
            MLKEMPublicKeyParameters pub = new MLKEMPublicKeyParameters(parameterSet(), peerPublicKey);
            MLKEMGenerator gen = new MLKEMGenerator(RANDOM);
            // generateEncapsulated returns SecretWithEncapsulation
            // packaging (sharedSecret, ciphertext). We copy out
            // both halves and let the BC wrapper go out of scope;
            // BC zeroes its internal buffer on collection.
            SecretWithEncapsulation packed = gen.generateEncapsulated(pub);
            byte[] secret = packed.getSecret();
            byte[] ciphertext = packed.getEncapsulation();
            return KemEncapsulation.builder()
                    .ciphertext(ciphertext.clone())
                    .sharedSecret(secret.clone())
                    .build();
        } catch (Exception ex) {
            throw new KemException(NAME, "encapsulation failed", ex);
        }
    }

    @Override
    public byte[] decapsulate(byte[] ciphertext, byte[] privateKey) {
        if (ciphertext == null || ciphertext.length != CIPHERTEXT_LEN) {
            throw new KemException(NAME,
                    "ciphertext has wrong length " +
                            (ciphertext == null ? "null" : String.valueOf(ciphertext.length)),
                    null);
        }
        if (privateKey == null) {
            throw new KemException(NAME, "private key must not be null", null);
        }
        try {
            MLKEMPrivateKeyParameters priv = new MLKEMPrivateKeyParameters(parameterSet(), privateKey);
            MLKEMExtractor extractor = new MLKEMExtractor(priv);
            return extractor.extractSecret(ciphertext);
        } catch (Exception ex) {
            throw new KemException(NAME, "decapsulation failed", ex);
        }
    }
}
