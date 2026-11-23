package com.studioquack.sqs.sandbox.oci;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * Points {@link SeccompProfile} at the daemon data directory ({@code SQS_DATA_DIR}), the same root
 * {@code MtlsIdentity} and {@code KemIkmStore} use.
 */
@Component
public class SeccompProfileBinder {

    public SeccompProfileBinder(@Value("${SQS_DATA_DIR:./data}") String dataDir) {
        SeccompProfile.configure(Path.of(dataDir));
    }
}
