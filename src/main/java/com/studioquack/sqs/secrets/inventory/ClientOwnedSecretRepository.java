package com.studioquack.sqs.secrets.inventory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/** The entity is database-agnostic and runs on the embedded the local database datasource. */
@Repository
public interface ClientOwnedSecretRepository extends JpaRepository<ClientOwnedSecretEntity, String> {
}
