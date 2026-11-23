package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BackendBaseResolver}: the outbound base seam with a bulletproof auto-fallback. Proves the
 * default returns public, {@code activateOverlay} flips to the overlay, N consecutive overlay
 * failures fall back to public, and a success resets the failure counter.
 */
class BackendBaseResolverTest {

    private static final String PUBLIC = "https://be.example.com/be/v1/sqvm/environments/env-1";
    private static final String OVERLAY = "https://203.0.113.10:443/be/v1/sqvm/environments/env-1";

    @Test
    void default_returns_public_base() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        assertEquals(PUBLIC, r.current(), "default (no overlay) returns the public base");
        assertFalse(r.overlayActive());
        assertEquals(PUBLIC, r.publicBase());
    }

    @Test
    void activate_overlay_returns_overlay_base() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        r.activateOverlay(OVERLAY);
        assertTrue(r.overlayActive());
        assertEquals(OVERLAY, r.current(), "after activateOverlay the overlay base is used");
    }

    @Test
    void blank_overlay_base_is_ignored() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        r.activateOverlay("   ");
        r.activateOverlay(null);
        assertFalse(r.overlayActive());
        assertEquals(PUBLIC, r.current(), "a blank/null overlay base leaves it on public");
    }

    @Test
    void flips_back_to_public_after_threshold_consecutive_failures() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC); // default threshold = 3
        r.activateOverlay(OVERLAY);

        r.reportFailure();
        r.reportFailure();
        assertTrue(r.overlayActive(), "still on overlay below the threshold");
        assertEquals(OVERLAY, r.current());

        r.reportFailure(); // 3rd consecutive → trip the fallback
        assertFalse(r.overlayActive(), "3 consecutive failures fall back to public");
        assertEquals(PUBLIC, r.current());
    }

    @Test
    void report_success_resets_the_failure_counter() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC); // threshold = 3
        r.activateOverlay(OVERLAY);

        r.reportFailure();
        r.reportFailure();
        r.reportSuccess(); // clears the accumulated flakiness
        r.reportFailure();
        r.reportFailure();
        assertTrue(r.overlayActive(), "success reset the counter, so 2 more failures do not trip it");
        assertEquals(OVERLAY, r.current());

        r.reportFailure(); // now 3 consecutive → trip
        assertFalse(r.overlayActive());
        assertEquals(PUBLIC, r.current());
    }

    @Test
    void gateway_errors_on_the_overlay_base_fall_back_to_public() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC); // threshold = 3
        r.activateOverlay(OVERLAY);

        r.reportResponse(401); // the backend itself answered — not a path failure
        r.reportResponse(502);
        r.reportResponse(504);
        assertTrue(r.overlayActive(), "two gateway errors stay below the threshold");

        r.reportResponse(503);
        assertFalse(r.overlayActive(), "a private-network path answering only 5xx gateway errors falls back");
        assertEquals(PUBLIC, r.current());
    }

    @Test
    void a_backend_answer_resets_gateway_error_count() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        r.activateOverlay(OVERLAY);

        r.reportResponse(502);
        r.reportResponse(502);
        r.reportResponse(500); // the backend answered (application error) — the path works
        r.reportResponse(502);
        r.reportResponse(502);
        assertTrue(r.overlayActive());
    }

    @Test
    void custom_threshold_is_honoured() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC, 2);
        r.activateOverlay(OVERLAY);
        r.reportFailure();
        assertTrue(r.overlayActive(), "1 failure below the custom threshold of 2");
        r.reportFailure();
        assertFalse(r.overlayActive(), "2 consecutive failures trip the custom threshold");
        assertEquals(PUBLIC, r.current());
    }

    @Test
    void non_positive_threshold_falls_back_to_default() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC, 0);
        r.activateOverlay(OVERLAY);
        r.reportFailure();
        r.reportFailure();
        assertTrue(r.overlayActive(), "default threshold (3) applied for a non-positive value");
        r.reportFailure();
        assertFalse(r.overlayActive());
    }

    @Test
    void failures_on_public_base_are_a_noop() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        r.reportFailure();
        r.reportFailure();
        r.reportFailure();
        r.reportFailure();
        assertFalse(r.overlayActive(), "reporting failures while public never changes state");
        assertEquals(PUBLIC, r.current());
    }

    @Test
    void deactivate_returns_to_public_and_can_re_arm() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC);
        r.activateOverlay(OVERLAY);
        r.deactivateOverlay();
        assertFalse(r.overlayActive());
        assertEquals(PUBLIC, r.current());

        // A fresh probe can re-arm the overlay after a public recovery.
        r.activateOverlay(OVERLAY);
        assertTrue(r.overlayActive());
        assertEquals(OVERLAY, r.current());
    }

    @Test
    void re_activate_after_fallback_resets_the_counter() {
        BackendBaseResolver r = new BackendBaseResolver(PUBLIC, 2);
        r.activateOverlay(OVERLAY);
        r.reportFailure();
        r.reportFailure(); // fell back to public
        assertFalse(r.overlayActive());

        r.activateOverlay(OVERLAY); // re-armed; counter reset
        r.reportFailure();
        assertTrue(r.overlayActive(), "re-activation reset the failure counter");
        assertEquals(OVERLAY, r.current());
    }
}
