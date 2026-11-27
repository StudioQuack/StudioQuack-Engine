package com.studioquack.sqs.webhook;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/** JPA repository for daemon-local {@link DaemonWebhookKeyEntity} records. */
public interface DaemonWebhookKeyRepository extends JpaRepository<DaemonWebhookKeyEntity, String> {

    Optional<DaemonWebhookKeyEntity> findByKeyId(String keyId);
}
