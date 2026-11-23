package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link OverlaySizeQuotaProbe}: the XFS-only {@code --storage-opt size=} gate parses
 * {@code podman info} graphStatus truthfully and fails SAFE (unsupported) on any bad answer,
 * so a stock ext4 Ubuntu host drops the disk cap instead of crashing every stateful start.
 */
class OverlaySizeQuotaProbeTest {

    /** A full {@code podman info --format json} document with the given backing filesystem. */
    private static String infoJson(String backingFs) {
        return "{\"host\":{\"ociRuntime\":{\"name\":\"crun\"}},"
                + "\"store\":{\"graphStatus\":{\"Backing Filesystem\":\"" + backingFs + "\"}}}";
    }

    @Test
    void parsesBackingFilesystem() {
        assertEquals("xfs", OverlaySizeQuotaProbe.backingFilesystem(
                "{\"Backing Filesystem\":\"xfs\",\"Native Overlay Diff\":\"true\"}"));
        assertEquals("extfs", OverlaySizeQuotaProbe.backingFilesystem(
                "{\"Backing Filesystem\":\"extfs\"}"));
        assertNull(OverlaySizeQuotaProbe.backingFilesystem("{}"));
        assertNull(OverlaySizeQuotaProbe.backingFilesystem("not json"));
        assertNull(OverlaySizeQuotaProbe.backingFilesystem(null));
        assertNull(OverlaySizeQuotaProbe.backingFilesystem("  "));
    }

    @Test
    void parsesGraphStatusOutOfTheFullInfoDocument() {
        // The probe reads the whole --format json document: a .store.* Go template is refused by
        // some Podman builds ("can't evaluate field store in type system.infoReport").
        assertEquals("{\"Backing Filesystem\":\"xfs\"}",
                OverlaySizeQuotaProbe.graphStatusJson(infoJson("xfs")));
        assertNull(OverlaySizeQuotaProbe.graphStatusJson("{}"));
        assertNull(OverlaySizeQuotaProbe.graphStatusJson("not json"));
        assertNull(OverlaySizeQuotaProbe.graphStatusJson(null));
    }

    @Test
    void xfsStoreSupportsSizeQuota() {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> new OciCli.Result(0, infoJson("xfs"), "", false);
        assertTrue(new OverlaySizeQuotaProbe(cli).sizeQuotaSupported());
        assertTrue(cli.sawCallContaining("info", "--format", "json"),
                "the whole JSON document is queried, not a version-fragile Go template");
    }

    @Test
    void extfsStoreDropsSizeQuota() {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> new OciCli.Result(0, infoJson("extfs"), "", false);
        assertFalse(new OverlaySizeQuotaProbe(cli).sizeQuotaSupported());
    }

    @Test
    void probeResultIsCachedForTheDaemonLifetime() {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> new OciCli.Result(0, infoJson("xfs"), "", false);
        OverlaySizeQuotaProbe probe = new OverlaySizeQuotaProbe(cli);
        assertTrue(probe.sizeQuotaSupported());
        int callsAfterFirst = cli.calls.size();
        assertTrue(probe.sizeQuotaSupported());
        assertEquals(callsAfterFirst, cli.calls.size(), "cached: no second podman info exec");
    }

    @Test
    void failsSafeWhenRuntimeUnavailableOrAnswerBad() {
        FakeOciCli down = new FakeOciCli();
        down.available = false;
        assertFalse(new OverlaySizeQuotaProbe(down).sizeQuotaSupported());

        // Default responder: exit 0 with empty stdout — unparseable → unsupported.
        assertFalse(new OverlaySizeQuotaProbe(new FakeOciCli()).sizeQuotaSupported());

        FakeOciCli failing = new FakeOciCli();
        failing.responder = argv -> new OciCli.Result(1, "", "boom", false);
        assertFalse(new OverlaySizeQuotaProbe(failing).sizeQuotaSupported());
    }
}
