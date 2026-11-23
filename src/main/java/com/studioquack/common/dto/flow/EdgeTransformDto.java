package com.studioquack.common.dto.flow;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
public class EdgeTransformDto implements Serializable {
    private int tabIndex;

    private String sourceNodeId;
    private String sourcePortId;
    private String sourcePath;
    private String sourcePathValue;
    private String sourceSchemaType;
    private String sourceDataType;
    private String sourcePortShortCode;
    private String sourceDescription;
    private String sourceWorkflowId;

    private String targetNodeId;
    private String targetPortId;
    private String targetPath;
    private String targetPathValue;
    private String targetSchemaType;
    private String targetDataType;
    private String targetPortShortCode;
    private String targetDescription;
    private String targetWorkflowId;

    /** FE catalog op name (e.g. toUpper, concat, default). Null/blank => plain copy (legacy). */
    private String operator;
    /** Ordered args after the first source path; see JoltTransformService arg encoding. */
    private List<String> args;
}
