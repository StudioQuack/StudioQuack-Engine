package com.studioquack.sqs.sandbox.oci;

/** Lets tests in other packages clear the process-wide {@link CgroupCaps} flags. */
public final class CgroupCapsTestSupport {

    private CgroupCapsTestSupport() {
    }

    public static void reset() {
        CgroupCaps.resetForTests();
    }
}
