package com.studioquack.sqs.sandbox.testrun;

import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Production broker start: plant the script, launch via {@code ProcessBuilder}, wait for the UDS.
 * Never throws — logs and returns null.
 */
@Slf4j
final class DefaultBrokerStarter implements BrokerStarter {

    private static final Duration PLANT_TIMEOUT = Duration.ofSeconds(15);

    private final OciCli cli;

    DefaultBrokerStarter(OciCli cli) {
        this.cli = cli;
    }

    @Override
    public HelperPipeChannel start(String cliPath, String containerId, HelperRequestRouter router) {
        try {
            OciCli.Result plant = cli.run(List.of(cliPath, "exec", containerId, "sh", "-c",
                    HelperBroker.plantScript()), PLANT_TIMEOUT);
            if (!plant.ok()) {
                log.warn("testbuild: helper-broker plant failed container={}: {}",
                        shortId(containerId), plant.firstErrLine());
                return null;
            }
            Process process = HelperBroker.launch(cliPath, containerId).start();
            OciCli.Result ready = cli.run(List.of(cliPath, "exec", containerId, "sh", "-c",
                    HelperBroker.readinessScript(10)), Duration.ofSeconds(15));
            if (!ready.ok()) {
                process.destroyForcibly();
                log.warn("testbuild: helper-broker not ready container={}", shortId(containerId));
                return null;
            }
            return new HelperPipeChannel(process, router);
        } catch (Exception e) {
            log.warn("testbuild: helper-broker start failed container={}: {}",
                    shortId(containerId), e.toString());
            return null;
        }
    }

    private static String shortId(String id) {
        if (id == null || id.isBlank()) {
            return "";
        }
        return id.length() <= 12 ? id : id.substring(0, 12);
    }
}
