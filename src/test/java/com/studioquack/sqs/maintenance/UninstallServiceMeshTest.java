package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.channel.net.NetAgentClient;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.warmpool.NoopWarmer;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The server {@code uninstall} and {@code local-uninstall} must log the node off the mesh while the
 * daemon is still the LocalAPI operator — the side-car may never get sudo to stop the service.
 */
class UninstallServiceMeshTest {

    @TempDir
    Path temp;

    @Test
    void purge_logsTheNodeOffTheMesh() throws Exception {
        ClientSecretInventoryService inventory = mock(ClientSecretInventoryService.class);
        when(inventory.purgeAllOwned()).thenReturn(0);
        ContainerRuntime runtime = mock(ContainerRuntime.class);
        when(runtime.available()).thenReturn(false);
        Path data = temp.resolve("data");
        Files.createDirectories(data);
        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());

        NetAgentClient agent = mock(NetAgentClient.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<NetAgentClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(agent);

        UninstallService service = new UninstallService(
                inventory, runtime, env, new WarmPool(new NoopWarmer(), Map.of()), provider);

        UninstallService.UninstallSummary summary = service.purge(false);

        verify(agent).down();
        verify(agent).logout();
        assertTrue(summary.notes().contains("mesh session down+logout"), summary.notes().toString());
    }
}
