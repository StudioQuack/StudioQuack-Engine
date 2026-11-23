package com.studioquack.common.dto.flow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EdgeDto implements Serializable {
    private String id;
    private String source;
    private String sourceHandle;
    private String target;
    private String targetHandle;
    private String type;
    private boolean animated;
    /**
     * Named {@code data} to match the React Flow library on the frontend.
     */
    private EdgeDataDto data;
}