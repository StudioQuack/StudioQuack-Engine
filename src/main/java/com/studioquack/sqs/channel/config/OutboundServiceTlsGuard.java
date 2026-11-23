package com.studioquack.sqs.channel.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Startup gate for the control-plane base URL, and a loud ignore of the removed TLS bypass keys so a leftover
 * installer flag cannot be mistaken for a working opt-out.
 */
@Slf4j
@Component
public class OutboundServiceTlsGuard {

    private final Environment env;

    public OutboundServiceTlsGuard(Environment env) {
        this.env = env;
    }

    @PostConstruct
    void enforce() {
        warnIfObsoleteTlsKeysPresent();
        HttpsEndpointPolicy.require(env.getProperty("sqs.backend.base-url"), "sqs.backend.base-url");
    }

    /**
     * The keys no longer bind to a field. If a unit file or {@code enrollment.properties} still sets them,
     * verification stays on and the operator is told to delete them.
     */
    private void warnIfObsoleteTlsKeysPresent() {
        if (present("STUDIOQUACK_TLS_INSECURE")
                || present("SQS_CHANNEL_TLS_INSECURE")
                || present("sqs.channel.tls-insecure")
                || present("sqs.dev-mode.tls-insecure")
                || present("sqs.channel.tls-ca-file")) {
            log.error("Obsolete TLS bypass/pin is set and ignored. Certificate and hostname "
                    + "verification stay on (JDK/OS trust store). Remove STUDIOQUACK_TLS_INSECURE, "
                    + "SQS_CHANNEL_TLS_INSECURE, sqs.channel.tls-insecure, sqs.dev-mode.tls-insecure "
                    + "and sqs.channel.tls-ca-file.");
        }
    }

    private boolean present(String key) {
        String v = env.getProperty(key);
        return v != null && !v.isBlank() && !v.equalsIgnoreCase("false") && !v.equals("0");
    }
}
