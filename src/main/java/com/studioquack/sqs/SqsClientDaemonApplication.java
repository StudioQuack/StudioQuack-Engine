package com.studioquack.sqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the self-hosted StudioQuack engine. One process runs the workflow engine, the
 * encrypted local database, the Podman sandbox, and local secrets for a single environment.
 */
@SpringBootApplication(scanBasePackages = {"com.studioquack.sqs"})
@EnableJpaRepositories(basePackages = {"com.studioquack.sqs"})
@EntityScan(basePackages = {"com.studioquack.sqs"})
@EnableAsync
@EnableScheduling
public class SqsClientDaemonApplication {

    public static void main(String[] args) {
        boolean localReverseLookups = com.studioquack.sqs.channel.net.OverlayReverseLookupProvider.install();
        System.out.println("==> private-network reverse lookups: "
                + (localReverseLookups ? "answered locally" : "by the OS resolver"));
        SpringApplication.run(SqsClientDaemonApplication.class, args);
    }
}
