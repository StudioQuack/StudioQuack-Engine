package com.studioquack.sqs.files;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Takes the file content the backend embeds in a {@code deploy} command and moves it into the
 * daemon's own {@link ClientFileStore}, leaving a {@code sq-file:} handle on the parameter.
 * Keeping that base64 in the deployed graph would mean the file sits in cleartext inside the
 * artifact store and gets re-serialised into every execution context that touches the node.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeployFileIngest {

    /** Marker the backend uses for an inflated file parameter (mirrors {@code NodeParameterFileEnricher}). */
    public static final String FILE_REF_KEY = "$fileRef";

    private static final String FILE_PARAM_TYPE = "file";

    private final ClientFileStore clientFileStore;

    /**
     * Called before the graph is persisted, so the artifact store never holds the base64.
     *
     * @return How many parameters were converted
     */
    public int ingest(String workflowId, List<NodeDto> nodes) {
        if (nodes == null || nodes.isEmpty() || workflowId == null || workflowId.isBlank()) {
            return 0;
        }
        // Supersede the previous deployment's files first: this deploy replaces them wholesale, and
        // leaving them would grow the store by one full set per deploy.
        clientFileStore.deleteScope(ClientFileStore.SCOPE_DEPLOY, workflowId);

        int converted = 0;
        for (NodeDto node : nodes) {
            TemplateDto template = node == null ? null : node.getData();
            if (template == null || template.getConfigurationParameters() == null) {
                continue;
            }
            for (ParameterDto parameter : template.getConfigurationParameters()) {
                if (parameter == null || !FILE_PARAM_TYPE.equalsIgnoreCase(parameter.getType())) {
                    continue;
                }
                String ref = convert(workflowId, node.getId(), parameter);
                if (ref != null) {
                    parameter.setValue(ref);
                    converted++;
                }
            }
        }
        if (converted > 0) {
            log.info("deploy.files.ingested workflowId={} files={}", workflowId, converted);
        }
        return converted;
    }

    /** @return the {@code sq-file:} handle, or {@code null} when there was nothing to convert. */
    @SuppressWarnings("unchecked")
    private String convert(String workflowId, String nodeId, ParameterDto parameter) {
        if (!(parameter.getValue() instanceof Map<?, ?> outer)) {
            return null;
        }
        Object inner = outer.get(FILE_REF_KEY);
        if (!(inner instanceof Map<?, ?> descriptorRaw)) {
            return null;
        }
        Map<String, Object> descriptor = (Map<String, Object>) descriptorRaw;
        Object base64 = descriptor.get("contentBase64");
        if (base64 == null) {
            // Leave it in place so the node can fail with a precise reason.
            log.warn("deploy.files: node={} parameter={} has no content ({}) — the node will not be "
                            + "able to read this file", nodeId, parameter.getId(),
                    descriptor.getOrDefault("omitted", "unknown"));
            return null;
        }
        try {
            byte[] content = Base64.getDecoder().decode(String.valueOf(base64));
            return clientFileStore.store(
                    ClientFileStore.SCOPE_DEPLOY,
                    workflowId,
                    String.valueOf(descriptor.getOrDefault("name", "file")),
                    descriptor.get("mimeType") == null ? null : String.valueOf(descriptor.get("mimeType")),
                    content);
        } catch (IllegalArgumentException rejected) {
            // Oversized or malformed base64. Deploy still succeeds: a graph that cannot carry one
            // file is more useful than no deployment at all, and the node reports the gap at run time.
            log.warn("deploy.files: node={} parameter={} could not be stored: {}",
                    nodeId, parameter.getId(), rejected.getMessage());
            return null;
        }
    }
}
