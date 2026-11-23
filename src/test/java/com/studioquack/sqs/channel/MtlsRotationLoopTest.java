package com.studioquack.sqs.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link MtlsRotationLoop}: the enrolment leaf is re-issued before expiry, a failed attempt is
 * retried hourly forever (never a gave-up state), and a daemon that booted bearer-only keeps
 * retrying until the server's cert service appears.
 */
class MtlsRotationLoopTest {

    /** Captures re-arm delays without ever running the scheduled task. */
    private static final class CapturingScheduler extends ScheduledThreadPoolExecutor {
        volatile long lastDelayMs = -1;

        CapturingScheduler() {
            super(0);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            lastDelayMs = unit.toMillis(delay);
            return null;
        }
    }

    private static X509Certificate selfSigned(String cn, Instant notAfter) throws Exception {
        BouncyCastleProvider bc = new BouncyCastleProvider();
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new java.security.spec.ECGenParameterSpec("secp256r1"));
        KeyPair kp = kpg.generateKeyPair();
        Instant nb = Instant.now().minus(1, ChronoUnit.MINUTES);
        JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(
                new X500Name("CN=" + cn), BigInteger.ONE, Date.from(nb), Date.from(notAfter),
                new X500Name("CN=" + cn), kp.getPublic());
        return new JcaX509CertificateConverter().setProvider(bc).getCertificate(
                b.build(new JcaContentSignerBuilder("SHA256withECDSA").setProvider(bc)
                        .build(kp.getPrivate())));
    }

    private static String pem(X509Certificate cert) throws Exception {
        return "-----BEGIN CERTIFICATE-----\n"
                + java.util.Base64.getMimeEncoder(64, "\n".getBytes())
                        .encodeToString(cert.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
    }

    /** An identity whose on-disk leaf expires at {@code leafNotAfter}. */
    private static MtlsIdentity enrolledIdentity(Path dataDir, Instant leafNotAfter) throws Exception {
        Path mtls = dataDir.resolve("mtls");
        Files.createDirectories(mtls);
        String pem = pem(selfSigned("sqvm-daemon-test", leafNotAfter));
        Files.writeString(mtls.resolve("cert.pem"), pem);
        Files.writeString(mtls.resolve("ca.pem"), pem);
        return MtlsIdentity.loadOrCreate(dataDir);
    }

    @Test
    void notDue_rearmsDailyWithoutEnrolling(@TempDir Path dir) throws Exception {
        MtlsIdentity id = enrolledIdentity(dir, Instant.now().plus(200, ChronoUnit.DAYS));
        CapturingScheduler scheduler = new CapturingScheduler();
        AtomicInteger enrolCalls = new AtomicInteger();
        MtlsRotationLoop loop = new MtlsRotationLoop(id, () -> {
            enrolCalls.incrementAndGet();
            return true;
        }, scheduler);
        loop.start();
        loop.tick();
        assertEquals(0, enrolCalls.get(), "a leaf with 200d left must not rotate");
        assertEquals(MtlsRotationLoop.CHECK_INTERVAL_MS, scheduler.lastDelayMs, "daily re-arm");
        loop.stop();
    }

    @Test
    void due_rotatesAndRearmsDaily(@TempDir Path dir) throws Exception {
        MtlsIdentity id = enrolledIdentity(dir, Instant.now().plus(10, ChronoUnit.DAYS));
        CapturingScheduler scheduler = new CapturingScheduler();
        AtomicInteger enrolCalls = new AtomicInteger();
        MtlsRotationLoop loop = new MtlsRotationLoop(id, () -> {
            enrolCalls.incrementAndGet();
            return true;
        }, scheduler);
        loop.start();
        loop.tick();
        assertEquals(1, enrolCalls.get(), "a leaf with 10d left must rotate");
        assertEquals(MtlsRotationLoop.CHECK_INTERVAL_MS, scheduler.lastDelayMs, "daily after success");
        loop.stop();
    }

    @Test
    void failedRotation_retriesHourlyForever(@TempDir Path dir) throws Exception {
        MtlsIdentity id = enrolledIdentity(dir, Instant.now().plus(10, ChronoUnit.DAYS));
        CapturingScheduler scheduler = new CapturingScheduler();
        AtomicInteger enrolCalls = new AtomicInteger();
        MtlsRotationLoop loop = new MtlsRotationLoop(id, () -> {
            enrolCalls.incrementAndGet();
            throw new IllegalStateException("BE unreachable");
        }, scheduler);
        loop.start();
        loop.tick();
        loop.tick();
        assertEquals(2, enrolCalls.get(), "every tick retries — the loop never gives up");
        assertEquals(MtlsRotationLoop.RETRY_INTERVAL_MS, scheduler.lastDelayMs, "hourly re-arm");
        loop.stop();
    }

    @Test
    void bearerOnlyBoot_retriesUntilTheCertServiceAppears(@TempDir Path dir) throws Exception {
        // No cert.pem/ca.pem on disk: enrol-mtls had answered 503 at bootstrap.
        MtlsIdentity id = MtlsIdentity.loadOrCreate(dir);
        CapturingScheduler scheduler = new CapturingScheduler();
        AtomicInteger enrolCalls = new AtomicInteger();
        MtlsRotationLoop loop = new MtlsRotationLoop(id, () ->
                enrolCalls.incrementAndGet() >= 2, scheduler);
        loop.start();
        loop.tick();
        assertEquals(MtlsRotationLoop.RETRY_INTERVAL_MS, scheduler.lastDelayMs,
                "unavailable enrol-mtls → hourly retry");
        loop.tick();
        assertEquals(2, enrolCalls.get());
        assertEquals(MtlsRotationLoop.CHECK_INTERVAL_MS, scheduler.lastDelayMs,
                "once enrolled, back to the daily cadence");
        loop.stop();
    }
}
