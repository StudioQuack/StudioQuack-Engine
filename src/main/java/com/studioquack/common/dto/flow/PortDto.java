package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.studioquack.common.enums.flow.PortType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PortDto implements Serializable {
    @Id
    private String id;
    private String name;
    private PortType type;
    private String value;
    private String description;
    private boolean required;
    @JsonProperty("isDefault")
    private boolean isDefault;
    private boolean expandable;
    private boolean instanceEditable;
    private Integer maxConnections;

    private List<SchemaDto> schema;
    /**
     * Used only for function ports.
     */
    private List<SchemaDto> inputSchema;
    private List<SchemaDto> outputSchema;
}
