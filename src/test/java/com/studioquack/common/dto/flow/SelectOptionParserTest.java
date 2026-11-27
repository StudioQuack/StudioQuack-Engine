package com.studioquack.common.dto.flow;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectOptionParserTest {

    @Test
    void parseOption_backwardCompatiblePlainAndLabelEntries() {
        var plain = SelectOptionParser.parseOption("send-message");
        assertEquals("send-message", plain.value());
        assertEquals("send-message", plain.label());
        assertEquals(SelectOptionRole.NORMAL, plain.role());

        var labeled = SelectOptionParser.parseOption("send-message:Send Message");
        assertEquals("send-message", labeled.value());
        assertEquals("Send Message", labeled.label());
        assertEquals(SelectOptionRole.NORMAL, labeled.role());

        var emptyLabel = SelectOptionParser.parseOption("listen::listen");
        assertEquals("listen", emptyLabel.value());
        assertEquals("listen", emptyLabel.label());
        assertEquals(SelectOptionRole.LISTEN, emptyLabel.role());
    }

    @Test
    void parseOption_recognizesAllSupportedRolesCaseInsensitively() {
        assertEquals(SelectOptionRole.LISTEN,
                SelectOptionParser.parseOption("listen:Listen (Incoming):listen").role());
        assertEquals(SelectOptionRole.MESSAGE_CATCH,
                SelectOptionParser.parseOption("wait_reply:Wait for Reply:message_catch").role());
        assertEquals(SelectOptionRole.SEND_AND_WAIT,
                SelectOptionParser.parseOption("send-and-wait:Send & Wait:send-and-wait").role());
        assertEquals(SelectOptionRole.MESSAGE_THROW,
                SelectOptionParser.parseOption("notify:Send Event:MESSAGE_THROW").role());
    }

    @Test
    void parseOption_preservesEscapedColonsAndUrls() {
        var escaped = SelectOptionParser.parseOption("urn\\:foo:URN Foo:listen");
        assertEquals("urn:foo", escaped.value());
        assertEquals("URN Foo", escaped.label());
        assertEquals(SelectOptionRole.LISTEN, escaped.role());

        var urlWithPort = SelectOptionParser.parseOption("https://example.com:8443:Production");
        assertEquals("https://example.com", urlWithPort.value());
        assertEquals("8443:Production", urlWithPort.label());
        assertEquals(SelectOptionRole.NORMAL, urlWithPort.role());

        var urlNoPort = SelectOptionParser.parseOption("https://example.com/hook:Webhook URL");
        assertEquals("https://example.com/hook", urlNoPort.value());
        assertEquals("Webhook URL", urlNoPort.label());
        assertEquals(SelectOptionRole.NORMAL, urlNoPort.role());
    }

    @Test
    void resolveActiveRole_and_findListenOption() {
        ParameterDto op = new ParameterDto();
        op.setShortCode("operation");
        op.setType("select");
        op.setOptions(List.of(
                "send-message:Send Message",
                "listen:Listen (Incoming):listen",
                "send-and-wait:Send & Wait:send_and_wait"));
        op.setValue("listen");

        TemplateDto tpl = new TemplateDto();
        tpl.setConfigurationParameters(List.of(op));

        assertEquals(SelectOptionRole.LISTEN, SelectOptionParser.resolveActiveRole(tpl));
        assertTrue(SelectOptionRole.LISTEN.blocksInputPort());
        assertTrue(SelectOptionRole.LISTEN.isIngressRole());
        assertTrue(SelectOptionParser.hasListenOption(tpl));
        assertEquals("operation", SelectOptionParser.findListenOption(tpl).orElseThrow().parameterShortCode());

        op.setValue("send-and-wait");
        assertEquals(SelectOptionRole.SEND_AND_WAIT, SelectOptionParser.resolveActiveRole(tpl));
        assertFalse(SelectOptionRole.SEND_AND_WAIT.blocksInputPort());
        assertTrue(SelectOptionRole.SEND_AND_WAIT.isIngressRole());

        op.setValue("send-message");
        assertEquals(SelectOptionRole.NORMAL, SelectOptionParser.resolveActiveRole(tpl));
    }
}
