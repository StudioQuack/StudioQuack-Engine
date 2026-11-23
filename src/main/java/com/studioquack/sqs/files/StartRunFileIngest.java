package com.studioquack.sqs.files;

import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.files.BackendConversationFileClient.FetchedFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Moves a chat attachment's inline bytes into the daemon's sealed file store at `start-run`. The
 * backend inlines an attachment's content because the daemon cannot read backend stored files.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StartRunFileIngest {

    private final ClientFileStore clientFileStore;
    /**
     * Present only on a channel-enabled daemon. Pulls the bytes of an attachment the backend judged
     * too large to inline, so a Docling / file node still receives real content.
     */
    private final Optional<BackendConversationFileClient> backendFileClient;

    /**
     * Rewrites inline attachment and voice content into handles, in place, and fetches back any
     * attachment the backend omitted (too large to inline) so it becomes a local handle too.
     *
     * @return how many blobs (attachments + voice) were moved into the store
     */
    public int ingest(String workflowExecutionId, ExecutionContext seedContext) {
        AgenticInfo agenticInfo = seedContext == null ? null : seedContext.getAgenticInfo();
        if (agenticInfo == null) {
            return 0;
        }
        int moved = 0;
        String conversationId = agenticInfo.getConversationId();
        List<ConversationFileRefDto> attachments = agenticInfo.getAttachments();
        if (attachments != null) {
            for (ConversationFileRefDto ref : attachments) {
                moved += ensureAttachmentLocal(workflowExecutionId, conversationId, ref);
            }
        }
        // Voice uses the same store path as attachments so the sealed context never holds multi-MB base64.
        // DuckbotBridge re-encodes the handle to a base64 string for the start `voices` port.
        moved += ensureLocal(workflowExecutionId, conversationId, agenticInfo.getVoice(), "voice");
        if (moved > 0) {
            log.info("start-run: moved {} chat file(s) into the client file store run={}",
                    moved, workflowExecutionId);
        }
        return moved;
    }

    /**
     * Makes one attachment's content available locally: inline bytes are moved into the store; an
     * omitted attachment (no inline bytes, still a stored files id) is fetched back over the secure
     * channel. A ref that already carries a {@code sq-file:} handle is left as-is.
     */
    private int ensureAttachmentLocal(String workflowExecutionId, String conversationId, ConversationFileRefDto ref) {
        return ensureLocal(workflowExecutionId, conversationId, ref, "attachment");
    }

    /**
     * Shared attachment/voice path: inline bytes are moved into the store; an omitted blob (no inline
     * bytes, still a stored files id) is fetched back over the secure channel. A ref that already
     * carries a {@code sq-file:} handle is left as-is.
     */
    private int ensureLocal(String workflowExecutionId, String conversationId, ConversationFileRefDto ref,
                            String label) {
        if (ref == null) {
            return 0;
        }
        if (ClientFileStore.isRef(ref.getFileId())) {
            return 0; // already a local handle (e.g. re-ingest)
        }
        if (ref.getContentBase64() != null && !ref.getContentBase64().isBlank()) {
            return storeAndClear(workflowExecutionId, ref, label);
        }
        return fetchAndStore(workflowExecutionId, conversationId, ref, label);
    }

    /**
     * Pulls an omitted attachment/voice blob's bytes from the backend and stores them under the run,
     * rewriting the ref to a {@code sq-file:} handle. No channel / no conversation / a failed fetch
     * leaves the named reference untouched so the node reports a missing file rather than crashing.
     */
    private int fetchAndStore(String workflowExecutionId, String conversationId, ConversationFileRefDto ref,
                              String label) {
        BackendConversationFileClient client = backendFileClient.orElse(null);
        if (client == null || conversationId == null || conversationId.isBlank()
                || ref.getFileId() == null || ref.getFileId().isBlank()) {
            return 0;
        }
        Optional<FetchedFile> fetched = client.fetch(workflowExecutionId, conversationId, ref.getFileId());
        if (fetched.isEmpty()) {
            return 0;
        }
        FetchedFile file = fetched.get();
        try {
            String handle = clientFileStore.store(
                    ClientFileStore.SCOPE_RUN, workflowExecutionId,
                    ref.getName() != null ? ref.getName() : file.name(),
                    ref.getMimeType() != null ? ref.getMimeType() : file.mimeType(),
                    file.content());
            ref.setFileId(handle);
            log.info("start-run: fetched oversized {} {} back from the backend run={}",
                    label, ref.getName(), workflowExecutionId);
            return 1;
        } catch (RuntimeException rejected) {
            log.warn("start-run: fetched {} {} could not be stored ({}); the node will see it "
                    + "as unavailable", label, ref.getName(), rejected.getMessage());
            return 0;
        }
    }

    /**
     * Stores one ref's inline bytes under the run scope and clears {@code contentBase64}.
     *
     * @return 1 when stored, 0 when skipped or failed
     */
    private int storeAndClear(String workflowExecutionId, ConversationFileRefDto ref, String label) {
        if (ref == null || ref.getContentBase64() == null || ref.getContentBase64().isBlank()) {
            // No content: either the backend judged it too large to inline, or the blob was
            // unreadable. The ref stays so the node can report a named, missing file rather than
            // failing on an absent value.
            return 0;
        }
        try {
            byte[] content = Base64.getDecoder().decode(ref.getContentBase64());
            String handle = clientFileStore.store(
                    ClientFileStore.SCOPE_RUN, workflowExecutionId,
                    ref.getName(), ref.getMimeType(), content);
            // The handle replaces the id so downstream resolution goes through the client store; the original
            // stored files id would mean nothing here.
            ref.setFileId(handle);
            return 1;
        } catch (RuntimeException rejected) {
            log.warn("start-run: {} {} could not be stored ({}); the node will see it as unavailable",
                    label, ref.getName(), rejected.getMessage());
            return 0;
        } finally {
            // Cleared either way — a blob that failed to store must not leave its base64
            // behind to be sealed into the context.
            ref.setContentBase64(null);
        }
    }
}
