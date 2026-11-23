package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaDto implements Serializable {
    @Id
    private String id;
    private String shortCode;
    private String label;
    private String type;
    private boolean required;
    private Object value;
    private String description;
    private List<SchemaDto> children;
}