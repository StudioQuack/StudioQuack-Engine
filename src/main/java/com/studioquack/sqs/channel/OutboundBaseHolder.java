package com.studioquack.sqs.channel;

import org.springframework.stereotype.Component;

/**
 * Shared handle to the live {@link BackendBaseResolver} so the daemon→BE run callbacks (status / sandbox-logs
 * / conversation-output) ride the SAME outbound base as heartbeat and exec-channel — i.e. the WireGuard
 * overlay hub when it is up, with automatic public-base fallback. The resolver is created inside
 * {@code SqsChannelBootstrap.bootstrap()} (after enrollment), not as a bean, so the callback beans cannot
 * inject it directly.
 */
@Component
public class OutboundBaseHolder {

    private volatile BackendBaseResolver resolver;

    /**
     * Bootstrap binds the live resolver once it exists (after enrollment + optional overlay repoint).
     */
    public void bind(BackendBaseResolver resolver) {
        this.resolver = resolver;
    }

    /**
     * The environment base outbound callbacks should use RIGHT NOW — the resolver's current base (overlay when
     * active, else public) once bound, else {@code publicEnvironmentBaseFallback}.
     */
    public String currentEnvironmentBase(String publicEnvironmentBaseFallback) {
        BackendBaseResolver r = this.resolver;
        return r != null ? r.current() : publicEnvironmentBaseFallback;
    }
}
