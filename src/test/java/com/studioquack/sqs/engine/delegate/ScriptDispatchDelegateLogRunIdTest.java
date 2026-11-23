package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.env.MockEnvironment;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A list-jobs row must name the run its sandbox logs are persisted under, or the engine page's
 * container-logs modal opens empty for that job.
 */
class ScriptDispatchDelegateLogRunIdTest {

    @Test
    void subProcessChildJob_namesTheTopLevelRunItsLogsArePostedUnder() {
        SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
        when(registry.resolveReportableRunId("child-run")).thenReturn("top-level-run");

        assertEquals("top-level-run", delegate(registry).logRunId("child-run"));
    }

    @Test
    void withoutTheRegistry_theRunIdIsKept() {
        assertEquals("run-1", delegate(null).logRunId("run-1"));
    }

    private static ScriptDispatchDelegate delegate(SubProcessCompletionRegistry registry) {
        return new ScriptDispatchDelegate(
                null, null, null, null, null, null, null, null, null,
                null,
                null,
                null,
                null,
                null, null, null, null, null, null, null, null, null, null,
                new MockEnvironment(),
                null,
                providerOf(null),
                null,
                providerOf(registry),
                null);
    }

    private static <T> ObjectProvider<T> providerOf(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return value == null ? Stream.empty() : Stream.of(value);
            }
        };
    }
}
