package com.studioquack.common.dto.flow;

import lombok.Data;
import java.time.LocalDateTime;
import java.util.List;

@Data
public class MessageContentDto {

    private String content;
    private int isActv;
    private int msgOrder;
    private String role;
    private Boolean positiveFeedback;

    /**
     * Legacy Docling text snippets kept so older conversations still load.
     * New writes use {@link #attachments}.
     */
    private List<String> userFiles;

    /**
     * Attachments on this message: user uploads and assistant output files.
     */
    private List<ConversationFileRefDto> attachments;

    private LocalDateTime eventTime;

}
