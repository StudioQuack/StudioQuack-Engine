package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Request and response transforms for a function-port call.
 * {@code request} maps the caller payload onto the sub-workflow input;
 * {@code response} maps the sub-workflow result back onto the caller's return shape.
 * An empty list is identity for that leg.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FunctionPortTransformsDto {

    /**
     * Specs targeting the request leg of {@code studioquack.execute(port, payload)}.
     * Order matters when multiple specs are emitted (chained
     * transforms apply in array order). Empty / null → identity.
     */
    @Builder.Default
    private List<TransformationSpecDto> request = new ArrayList<>();

    /** Response-leg specs, applied to the sub-workflow result. Empty or null means identity. */
    @Builder.Default
    private List<TransformationSpecDto> response = new ArrayList<>();
}
