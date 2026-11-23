package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * CPU, memory, disk, image, and bespoke-file hints for a node template.
 * Unknown properties are ignored so older templates still deserialize.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionRequirementsDto implements Serializable {
    private int cpu;
    private int ram;
    private int disc;

    /** Primary script language. Dispatch uses {@code templateScript.language}. */
    private String scriptLanguage;

    /** OCI image this node runs on. Blank means the language's default image. */
    private String ociImageRef;

    /**
     * Deprecated no-op for dispatch. Kept for already-deployed templates.
     * Routing is image-based: live stateful instance → warm pool → cold start.
     */
    private boolean stateful;

    /** Metadata for user-uploaded bespoke handler files. May be null on older payloads. */
    private List<Object> bespokeFiles = new ArrayList<>();
}