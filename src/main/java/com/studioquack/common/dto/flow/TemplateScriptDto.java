package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemplateScriptDto implements Serializable,Cloneable {
    private String id;
    private String templateId;
    private String language;
    private String script;

    @Override
    public TemplateScriptDto clone() {
        try {
            return (TemplateScriptDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
