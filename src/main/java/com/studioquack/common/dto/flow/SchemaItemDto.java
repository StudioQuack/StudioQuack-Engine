package com.studioquack.common.dto.flow;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class SchemaItemDto implements Serializable {
    private String id;
    private String type;
    private String nodeId;
    private String portId;
    private String paramType;
    private String shortCode;
    private String schemaName;
    private Integer[] levelPath;
    private Object value;
    private String description;
}
