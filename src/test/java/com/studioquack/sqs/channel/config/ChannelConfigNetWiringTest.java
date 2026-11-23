package com.studioquack.sqs.channel.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.CliNetAgentClient;
import com.studioquack.sqs.channel.net.NetAgentClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Data-plane selection: CLI answers {@code version} → {@link CliNetAgentClient};
 * missing CLI → {@code null} ({@code no-data-plane}).
 */
class ChannelConfigNetWiringTest {

    @Test
    void noDataPlane_whenCliAbsent(@TempDir Path dataDir) {
        SqsChannelProperties props = props(dataDir);
        props.getNet().setCliPath("/definitely/missing/sq-net");

        assertNull(ChannelConfig.selectDataPlane(props, new ObjectMapper()),
                "absent sq-net must yield NO data plane");
    }

    @Test
    void selectsCli_whenVersionSucceeds(@TempDir Path dataDir) throws Exception {
        Path cli = dataDir.resolve("sq-net");
        Files.writeString(cli, """
                #!/bin/sh
                if [ "$1" = "version" ]; then
                  printf '1.102.0-sq-net\\n'
                  exit 0
                fi
                exit 1
                """);
        try {
            Files.setPosixFilePermissions(cli, PosixFilePermissions.fromString("rwxr-xr-x"));
        } catch (UnsupportedOperationException ignored) {
            cli.toFile().setExecutable(true);
        }
        SqsChannelProperties props = props(dataDir);
        props.getNet().setCliPath(cli.toAbsolutePath().toString());

        NetAgentClient client = ChannelConfig.selectDataPlane(props, new ObjectMapper());
        assertInstanceOf(CliNetAgentClient.class, client);
    }

    private static SqsChannelProperties props(Path dataDir) {
        SqsChannelProperties p = new SqsChannelProperties();
        p.setEnabled(true);
        p.setDataDir(dataDir.toString());
        return p;
    }
}
