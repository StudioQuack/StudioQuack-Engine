package com.studioquack.sqs.files;

import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.files.BackendConversationFileClient.FetchedFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Chat attachments arriving inline on `start-run`. The ordering guarantee is the point: this runs
 * BEFORE the execution context is snapshotted, so multi-MB base64 never reaches the sealed context
 * — which is written once per run and re-read on every node hop.
 */
class StartRunFileIngestTest {

    private ClientFileStore store;
    private BackendConversationFileClient backendFileClient;
    private StartRunFileIngest ingest;

    @BeforeEach
    void setUp() {
        store = mock(ClientFileStore.class);
        when(store.store(anyString(), anyString(), any(), any(), any()))
                .thenReturn(ClientFileStore.REF_PREFIX + "stored-1");
        backendFileClient = mock(BackendConversationFileClient.class);
        // Default: no backend fetch available, matching a channel-less daemon.
        ingest = new StartRunFileIngest(store, Optional.empty());
    }

    private static ConversationFileRefDto attachment(String fileId, String name, String content) {
        ConversationFileRefDto ref = new ConversationFileRefDto();
        ref.setFileId(fileId);
        ref.setName(name);
        ref.setMimeType("application/pdf");
        if (content != null) {
            ref.setContentBase64(Base64.getEncoder()
                    .encodeToString(content.getBytes(StandardCharsets.UTF_8)));
        }
        return ref;
    }

    private static ExecutionContext contextWith(ConversationFileRefDto... refs) {
        AgenticInfo info = new AgenticInfo();
        info.setConversationId("conv-1");
        info.setAttachments(new ArrayList<>(List.of(refs)));
        ExecutionContext ctx = new ExecutionContext();
        ctx.setAgenticInfo(info);
        return ctx;
    }

    @Test
    @DisplayName("inline content is moved into the store and the ref carries a handle")
    void inlineContent_becomesHandle() {
        ExecutionContext ctx = contextWith(attachment("gridfs-1", "invoice.pdf", "bytes"));

        assertEquals(1, ingest.ingest("run-1", ctx));

        ConversationFileRefDto ref = ctx.getAgenticInfo().getAttachments().get(0);
        assertTrue(ClientFileStore.isRef(ref.getFileId()), "the GridFS id is replaced by a client handle");
        assertNull(ref.getContentBase64(), "the base64 must not survive into the sealed context");
    }

    @Test
    @DisplayName("attachments are stored under the run so terminal cleanup reclaims them")
    void storedUnderRunScope() {
        ingest.ingest("run-42", contextWith(attachment("gridfs-1", "a.pdf", "x")));

        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-42"), eq("a.pdf"), any(), any());
    }

    @Test
    @DisplayName("a ref the backend could not inline keeps its id when no backend fetch is available")
    void refWithoutContent_isLeftAlone() {
        // Better a named, missing file than a value that silently disappears.
        ExecutionContext ctx = contextWith(attachment("gridfs-toolarge", "huge.pdf", null));

        assertEquals(0, ingest.ingest("run-1", ctx));

        assertEquals("gridfs-toolarge", ctx.getAgenticInfo().getAttachments().get(0).getFileId());
        verify(store, never()).store(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("an omitted attachment is fetched back from the backend and becomes a local handle")
    void omittedAttachment_isFetchedAndStored() {
        // The 5 MB-GIF case: too large to inline, so the backend sent the reference only. With a
        // channel present the daemon pulls the bytes back so a Docling / file node still gets content.
        when(backendFileClient.fetch(eq("run-1"), eq("conv-1"), eq("gridfs-toolarge")))
                .thenReturn(Optional.of(new FetchedFile("huge.gif", "image/gif", "real-bytes".getBytes(StandardCharsets.UTF_8))));
        ingest = new StartRunFileIngest(store, Optional.of(backendFileClient));
        ExecutionContext ctx = contextWith(attachment("gridfs-toolarge", "huge.gif", null));

        assertEquals(1, ingest.ingest("run-1", ctx));

        ConversationFileRefDto ref = ctx.getAgenticInfo().getAttachments().get(0);
        assertTrue(ClientFileStore.isRef(ref.getFileId()), "the fetched file is now a local handle");
        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("huge.gif"), any(), any());
    }

    @Test
    @DisplayName("an omitted attachment the backend will not return keeps its id (fail-open reference)")
    void omittedAttachment_fetchEmpty_isLeftAlone() {
        when(backendFileClient.fetch(anyString(), anyString(), anyString())).thenReturn(Optional.empty());
        ingest = new StartRunFileIngest(store, Optional.of(backendFileClient));
        ExecutionContext ctx = contextWith(attachment("gridfs-toolarge", "huge.gif", null));

        assertEquals(0, ingest.ingest("run-1", ctx));

        assertEquals("gridfs-toolarge", ctx.getAgenticInfo().getAttachments().get(0).getFileId());
        verify(store, never()).store(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("a failed store still clears the base64")
    void storeFailure_clearsTheBase64() {
        // Otherwise the bytes that could not be stored would be sealed into the context anyway, which
        // is the exact outcome this class exists to prevent.
        when(store.store(anyString(), anyString(), any(), any(), any()))
                .thenThrow(new IllegalArgumentException("too big"));
        ExecutionContext ctx = contextWith(attachment("gridfs-1", "a.pdf", "x"));

        assertEquals(0, ingest.ingest("run-1", ctx));

        assertNull(ctx.getAgenticInfo().getAttachments().get(0).getContentBase64());
    }

    @Test
    @DisplayName("several attachments are each moved")
    void multipleAttachments() {
        ExecutionContext ctx = contextWith(
                attachment("g1", "a.pdf", "one"),
                attachment("g2", "b.pdf", "two"));

        assertEquals(2, ingest.ingest("run-1", ctx));
    }

    @Test
    @DisplayName("a run with no attachments is a no-op")
    void noAttachments() {
        assertEquals(0, ingest.ingest("run-1", new ExecutionContext()));
        assertEquals(0, ingest.ingest("run-1", null));
        verify(store, never()).store(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("inline voice is moved into the store and contentBase64 is cleared")
    void voice_becomesHandle() {
        AgenticInfo info = new AgenticInfo();
        info.setConversationId("conv-1");
        ConversationFileRefDto voice = attachment("gridfs-voice", "clip.webm", "audio-bytes");
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        info.setVoice(voice);
        ExecutionContext ctx = new ExecutionContext();
        ctx.setAgenticInfo(info);

        assertEquals(1, ingest.ingest("run-1", ctx));

        ConversationFileRefDto stored = ctx.getAgenticInfo().getVoice();
        assertTrue(ClientFileStore.isRef(stored.getFileId()), "voice GridFS id is replaced by a client handle");
        assertNull(stored.getContentBase64(), "voice base64 must not survive into the sealed context");
        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("clip.webm"), any(), any());
    }

    @Test
    @DisplayName("an omitted voice clip is fetched back from the backend like an attachment")
    void omittedVoice_isFetchedAndStored() {
        // The backend inlines voice only up to its voice ceiling; above it the start-run carries
        // the bare GridFS reference. Without the fetch-back the start `voices` port published an
        // empty list and a Docling node wired to it saw "no document".
        when(backendFileClient.fetch(eq("run-1"), eq("conv-1"), eq("gridfs-voice-big")))
                .thenReturn(Optional.of(new FetchedFile("clip.webm", "audio/webm",
                        "real-audio".getBytes(StandardCharsets.UTF_8))));
        ingest = new StartRunFileIngest(store, Optional.of(backendFileClient));
        AgenticInfo info = new AgenticInfo();
        info.setConversationId("conv-1");
        ConversationFileRefDto voice = attachment("gridfs-voice-big", "clip.webm", null);
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        info.setVoice(voice);
        ExecutionContext ctx = new ExecutionContext();
        ctx.setAgenticInfo(info);

        assertEquals(1, ingest.ingest("run-1", ctx));

        ConversationFileRefDto stored = ctx.getAgenticInfo().getVoice();
        assertTrue(ClientFileStore.isRef(stored.getFileId()), "the fetched voice is now a local handle");
        verify(store).store(eq(ClientFileStore.SCOPE_RUN), eq("run-1"), eq("clip.webm"), any(), any());
    }

    @Test
    @DisplayName("an omitted voice clip the backend will not return keeps its id (fail-open reference)")
    void omittedVoice_fetchEmpty_isLeftAlone() {
        when(backendFileClient.fetch(anyString(), anyString(), anyString())).thenReturn(Optional.empty());
        ingest = new StartRunFileIngest(store, Optional.of(backendFileClient));
        AgenticInfo info = new AgenticInfo();
        info.setConversationId("conv-1");
        ConversationFileRefDto voice = attachment("gridfs-voice-big", "clip.webm", null);
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        info.setVoice(voice);
        ExecutionContext ctx = new ExecutionContext();
        ctx.setAgenticInfo(info);

        assertEquals(0, ingest.ingest("run-1", ctx));

        assertEquals("gridfs-voice-big", ctx.getAgenticInfo().getVoice().getFileId());
        verify(store, never()).store(anyString(), anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("voice and attachments are both moved in one ingest pass")
    void voiceAndAttachments() {
        AgenticInfo info = new AgenticInfo();
        info.setConversationId("conv-1");
        info.setAttachments(new ArrayList<>(List.of(attachment("g1", "a.pdf", "one"))));
        ConversationFileRefDto voice = attachment("g-voice", "clip.webm", "audio");
        voice.setMimeType("audio/webm");
        voice.setKind("audio");
        info.setVoice(voice);
        ExecutionContext ctx = new ExecutionContext();
        ctx.setAgenticInfo(info);

        assertEquals(2, ingest.ingest("run-1", ctx));
        assertNull(ctx.getAgenticInfo().getAttachments().get(0).getContentBase64());
        assertNull(ctx.getAgenticInfo().getVoice().getContentBase64());
    }
}
